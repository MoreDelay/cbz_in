//! Contains everything related to handling zip archives.

use std::fs::{self, File};
use std::io::BufRead;
use std::io::Read;
use std::io::Write;
use std::ops::Deref;
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};
use walkdir::WalkDir;
use zip::CompressionMethod;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use exn::{Exn, ResultExt};
use indicatif::ProgressBar;
use tracing::debug;

use crate::convert::Configuration;
use crate::convert::dir::TempDirGuard;
use crate::convert::image::{ConversionJob, ConversionJobs, ImageFormat, Plan};
use crate::convert::search::{ArchiveImages, ImageInfo};
use crate::error::{ErrorMessage, NothingToDo};
use crate::spawn;

/// Represents the job to convert all images within a Zip archive.
///
/// When run, this job extracts the archive into a temporary directory next to the archive,
/// converts all files inside that directory, then compresses the archive again. The final archive
/// is placed next to the original, with an additional suffix to its name.
pub struct ArchiveJob {
    /// The path to the archive on which this job operates.
    archive_path: ArchivePath,
    /// The extraction job for the archive.
    extraction: ExtractionJob,
    /// The conversion jobs to convert all images.
    conversion: ConversionJobs,
    /// The job to compress the archive back into a Zip file again.
    compression: CompressionJob,
}

impl super::Job for ArchiveJob {
    fn path(&self) -> &Path {
        &self.archive_path
    }

    /// Run this job.
    fn run(self, bar: &ProgressBar) -> Result<(), Exn<ErrorMessage>> {
        let Self {
            archive_path,
            extraction,
            conversion,
            compression,
        } = self;

        let err = || {
            let path = archive_path.deref();
            ErrorMessage::new(format!("Failed to convert images in archive {path:?}",))
        };

        let _guard = extraction.run().or_raise(err)?;
        conversion.run(bar).or_raise(err)?;
        compression.run().or_raise(err)?;

        Ok(())
    }
}

impl ArchiveJob {
    /// Create a new job to convert all images in an archive.
    ///
    /// No files get touched until this job is run.
    pub fn new(
        archive: ArchiveImages,
        config: &Configuration,
    ) -> Result<Result<Self, Exn<NothingToDo>>, Exn<ErrorMessage>> {
        let ArchiveImages { archive, images } = archive;

        let err = || {
            ErrorMessage::new(format!(
                "Failed to prepare job for archive conversion {archive:?}"
            ))
        };

        let &Configuration {
            target, n_workers, ..
        } = config;

        if Self::already_converted(&archive, target).or_raise(err)? {
            let msg = format!("Already converted {archive:?}");
            let exn = Exn::new(NothingToDo::new(msg));
            return Ok(Err(exn));
        }

        let extract_dir = Self::get_conversion_root_dir(&archive);
        if extract_dir.exists() {
            let msg = format!("Extract directory already exists at {archive:?}");
            let exn = Exn::new(ErrorMessage::new(msg));
            return Err(exn);
        }

        let root_dir = Self::get_extraction_root_dir(&archive).or_raise(err)?;
        let job_queue = images
            .into_iter()
            .filter_map(|ImageInfo { path, format }| {
                let image_path = root_dir.join(path);
                match Plan::new(format, config) {
                    Some(task) => {
                        debug!("create job for {image_path:?}: {task:?}");
                        Some(ConversionJob::new(image_path, task))
                    }
                    None => {
                        debug!("skip conversion for {image_path:?}");
                        None
                    }
                }
            })
            .collect::<VecDeque<_>>();

        if job_queue.is_empty() {
            let msg = format!("No files to convert in {archive:?}");
            let exn = Exn::new(NothingToDo::new(msg));
            return Ok(Err(exn));
        }

        let extraction = ExtractionJob {
            archive_path: archive.clone(),
        };
        let conversion = ConversionJobs::new(job_queue, n_workers);
        let compression = CompressionJob {
            root: Self::get_conversion_root_dir(&archive),
            target,
        };
        Ok(Ok(Self {
            archive_path: archive,
            extraction,
            conversion,
            compression,
        }))
    }

    /// Builds the path to the temporary directory where the archive gets extracted to.
    fn get_conversion_root_dir(cbz_path: &ArchivePath) -> PathBuf {
        let dir = cbz_path.parent().unwrap();
        let name = cbz_path.file_stem().unwrap();
        dir.join(name)
    }

    /// Checks if the given archive has already been converted.
    ///
    /// A converted archive either already holds the correct image format suffix in its name, or
    /// there exists another archive with the same name and that suffix in the same directory.
    fn already_converted(
        path: &ArchivePath,
        target: ImageFormat,
    ) -> Result<bool, Exn<ErrorMessage>> {
        let err =
            || ErrorMessage::new("Could not check if archive has been converted before: {path:?}");

        let conversion_ending = format!(".{}.cbz", target.ext());

        let dir = path.parent().unwrap();
        let name = path.file_stem().unwrap();
        let zip_path = dir.join(format!("{}{}", name.to_str().unwrap(), conversion_ending));

        let is_converted_archive = path.to_str().unwrap().ends_with(&conversion_ending);
        let has_converted_archive = zip_path.try_exists().or_raise(err)?;

        Ok(is_converted_archive || has_converted_archive)
    }

    /// Builds the path that should be provided for 7z such that it aligns with our expectations.
    ///
    /// When the archive gets compressed later, it should include a single top-level directory with
    /// the same file name as the archive itself, without extensions. If the archive already has
    /// this directory, then it can be extracted directly. Otherwise the root directory must be
    /// created first, and the contents are extracted inside of that.
    fn get_extraction_root_dir(cbz_path: &ArchivePath) -> Result<PathBuf, Exn<ErrorMessage>> {
        let err = || {
            let msg = format!("Could not determine the extraction dir for {cbz_path:?}");
            ErrorMessage::new(msg)
        };

        let archive_name = cbz_path.file_stem().unwrap();
        let archive_root_dirs = spawn::list_archive_files(cbz_path)
            .and_then(|c| c.wait_with_output())
            .or_raise(err)?
            .stdout
            .lines()
            .filter(|v| v.as_ref().is_ok_and(|line| line.starts_with("Path = ")))
            .map(|v| v.unwrap().strip_prefix("Path = ").unwrap().to_string())
            .filter(|file| !file.contains("/"))
            .collect::<Vec<_>>();

        let has_root_within =
            archive_root_dirs.len() == 1 && *archive_root_dirs[0] == *archive_name;
        let extract_dir = match has_root_within {
            true => {
                let parent_dir = cbz_path.parent().unwrap().to_path_buf();
                assert_eq!(
                    parent_dir.join(archive_name),
                    Self::get_conversion_root_dir(cbz_path)
                );
                parent_dir
            }
            false => Self::get_conversion_root_dir(cbz_path),
        };
        Ok(extract_dir)
    }
}

/// The job to extract an archive into a temporary directory.
///
/// The temporary directory is placed next to the archive and will have the same name as the
/// archive, just missing its extensions.
struct ExtractionJob {
    /// The archive to be extracted.
    archive_path: ArchivePath,
}

impl ExtractionJob {
    /// Run this job.
    fn run(self) -> Result<TempDirGuard, Exn<ErrorMessage>> {
        let err = || {
            let path = &self.archive_path;
            ErrorMessage::new(format!("Failed to extract the archive {path:?}"))
        };

        assert!(self.archive_path.is_file());

        let extract_dir = ArchiveJob::get_conversion_root_dir(&self.archive_path);
        assert!(!extract_dir.exists());

        let guard = TempDirGuard::new(extract_dir.to_path_buf());
        fs::create_dir_all(&extract_dir)
            .or_raise(|| {
                let msg = format!("Could not create the target directory at {extract_dir:?}");
                ErrorMessage::new(msg)
            })
            .or_raise(err)?;
        spawn::extract_zip(&self.archive_path, &extract_dir)
            .and_then(|c| c.wait())
            .or_raise(err)?;
        Ok(guard)
    }
}

/// The job to compress a directory into an archive.
///
/// The archive name will have the same name as the directory, extended with `.<image-format>.cbz`.
struct CompressionJob {
    /// The root directory to compress into a Zip file.
    root: PathBuf,
    /// The target image format which becomes part of the archive's file name.
    target: ImageFormat,
}

impl CompressionJob {
    /// Run this job.
    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        let err = || {
            let root = &self.root;
            ErrorMessage::new(format!("Failed to compress the directory {root:?}"))
        };

        let dir = self
            .root
            .parent()
            .expect("root is a temporary directory, so it has a parent");
        let name = self
            .root
            .file_stem()
            .expect("root is a temporary directory with a name");
        let zip_path = dir.join(format!(
            "{}.{}.cbz",
            name.to_str().expect("our file paths are utf8 compliant"),
            self.target.ext()
        ));

        let file = File::create(zip_path).or_raise(err)?;

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        let mut buffer = Vec::new();
        for entry in WalkDir::new(&self.root).into_iter() {
            let entry = entry.or_raise(err)?;
            let entry = entry.path();
            let root_parent = self
                .root
                .parent()
                .expect("root dir is a temporary directory, not root");
            let inner_path = entry
                .strip_prefix(root_parent)
                .expect("all files have the root as prefix")
                .to_str()
                .expect("path should be utf8 compliant");

            if entry.is_file() {
                zipper.start_file(inner_path, options).or_raise(err)?;
                File::open(entry)
                    .and_then(|mut f| f.read_to_end(&mut buffer))
                    .or_raise(err)?;
                zipper.write_all(&buffer).or_raise(err)?;
                buffer.clear();
            } else if !inner_path.is_empty() {
                zipper.add_directory(inner_path, options).or_raise(err)?;
            }
        }

        zipper.finish().or_raise(err)?;
        Ok(())
    }
}

/// A path that was verified to point to an existing Zip archive.
#[derive(Debug, Clone)]
pub struct ArchivePath(PathBuf);

impl std::ops::Deref for ArchivePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ArchivePath {
    /// All valid Zip archive extensions we consider.
    const ARCHIVE_EXTENSIONS: [&str; 2] = ["zip", "cbz"];

    /// Checked constructor to verify the path points to an archive.
    ///
    /// This only checks that the directory exists at the time of creation.
    pub fn new(archive_path: PathBuf) -> Result<Self, Exn<ErrorMessage, PathBuf>> {
        let correct_extension = archive_path.extension().is_some_and(|ext| {
            Self::ARCHIVE_EXTENSIONS
                .iter()
                .any(|valid_ext| ext.eq_ignore_ascii_case(valid_ext))
        });
        if !correct_extension {
            let msg = format!("Archive has an unsupported extension: {archive_path:?}");
            let exn = Exn::with_recovery(ErrorMessage::new(msg), archive_path);
            return Err(exn);
        }

        if !archive_path.is_file() {
            let msg = format!("Archive does not exist: {archive_path:?}");
            let exn = Exn::with_recovery(ErrorMessage::new(msg), archive_path);
            return Err(exn);
        }

        Ok(ArchivePath(archive_path))
    }
}
