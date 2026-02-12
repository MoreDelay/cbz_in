//! Contains everything related to handling zip archives.

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead as _, Write as _};
use std::path::{Path, PathBuf};

use exn::{ErrorExt as _, Exn, ResultExt as _, bail};
use indicatif::ProgressBar;
use tracing::debug;
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use crate::convert::ConversionConfig;
use crate::convert::dir::TempDirGuard;
use crate::convert::image::{ConversionJob, ConversionJobs, ImageFormat, Plan};
use crate::convert::search::{ArchiveImages, ImageInfo};
use crate::error::{ErrorMessage, NothingToDo};
use crate::spawn::{self, ManagedChild};

/// Represents the job to convert all images within a Zip archive.
///
/// When run, this job extracts the archive into a temporary directory next to the archive,
/// converts all files inside that directory, then compresses the archive again. The final archive
/// is placed next to the original, with an additional suffix to its name.
pub struct ArchiveJob {
    /// The path to the archive on which this job operates.
    archive: ArchivePath,
    /// The extraction job for the archive.
    extraction: ExtractionJob,
    /// The conversion jobs to convert all images.
    conversion: ConversionJobs,
    /// The job to compress the archive back into a Zip file again.
    compression: CompressionJob,
}

impl super::Job for ArchiveJob {
    fn path(&self) -> &Path {
        &self.archive
    }

    fn iter(&self) -> impl Iterator<Item = &ConversionJob> {
        self.conversion.iter()
    }

    /// Run this job.
    fn run(self, bar: &ProgressBar) -> Result<(), Exn<ErrorMessage>> {
        let Self {
            archive,
            extraction,
            conversion,
            compression,
        } = self;

        let err = || {
            let archive = archive.display();
            ErrorMessage::new(format!("Failed to convert images in archive \"{archive}\""))
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
        config: ConversionConfig,
    ) -> Result<Result<Self, Exn<NothingToDo>>, Exn<ErrorMessage>> {
        let ArchiveImages { archive, images } = archive;

        let err = || {
            let archive = archive.display();
            ErrorMessage::new(format!(
                "Failed to prepare job for archive conversion \"{archive}\""
            ))
        };

        let ConversionConfig {
            target, n_workers, ..
        } = config;

        if Self::already_converted(&archive, target).or_raise(err)? {
            let archive = archive.display();
            let msg = format!("Already converted \"{archive}\"");
            let exn = Exn::new(NothingToDo::new(msg));
            return Ok(Err(exn));
        }

        let root_dir = Self::get_extraction_root_dir(&archive).or_raise(err)?;
        let job_queue = images
            .into_iter()
            .filter_map(|ImageInfo { path, format }| {
                let image_path = root_dir.join(path);
                if let Some(task) = Plan::new(format, config) {
                    debug!("create job for {image_path:?}: {task:?}");
                    Some(ConversionJob::new(image_path, task))
                } else {
                    debug!("skip conversion for {image_path:?}");
                    None
                }
            })
            .collect::<VecDeque<_>>();

        if job_queue.is_empty() {
            let archive = archive.display();
            let msg = format!("No files to convert in \"{archive}\"");
            let exn = Exn::new(NothingToDo::new(msg));
            return Ok(Err(exn));
        }

        let extraction = ExtractionJob {
            archive: archive.clone(),
        };
        let conversion = ConversionJobs::new(job_queue, n_workers);
        let compression = CompressionJob {
            root: Self::get_conversion_root_dir(&archive),
            target,
            extension: archive.extension,
        };
        Ok(Ok(Self {
            archive,
            extraction,
            conversion,
            compression,
        }))
    }

    /// Builds the path to the temporary directory where the archive gets extracted to.
    fn get_conversion_root_dir(archive: &ArchivePath) -> PathBuf {
        let dir = archive.parent();
        let name = archive.name();
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
        let err = || {
            let path = path.display();
            ErrorMessage::new(format!(
                "Could not check if archive has been converted before: \"{path}\""
            ))
        };

        let conversion_ending = format!(".{}.cbz", target.ext());

        let dir = path.parent();
        let name = path.name().to_str().expect("files should be unicode");
        let zip_path = dir.join(format!("{name}{conversion_ending}"));

        let is_converted_archive = path.ends_with(&conversion_ending);
        let has_converted_archive = zip_path.try_exists().or_raise(err)?;

        Ok(is_converted_archive || has_converted_archive)
    }

    /// Builds the path that should be provided for 7z such that it aligns with our expectations.
    ///
    /// When the archive gets compressed later, it should include a single top-level directory with
    /// the same file name as the archive itself, without extensions. If the archive already has
    /// this directory, then it can be extracted directly. Otherwise the root directory must be
    /// created first, and the contents are extracted inside of that.
    fn get_extraction_root_dir(archive: &ArchivePath) -> Result<PathBuf, Exn<ErrorMessage>> {
        let err = || {
            let archive = archive.display();
            let msg = format!("Could not determine the extraction dir for \"{archive}\"");
            ErrorMessage::new(msg)
        };

        let name = archive.name();
        let root_dirs = spawn::list_archive_files(archive)
            .and_then(ManagedChild::wait_with_output)
            .or_raise(err)?
            .stdout
            .lines()
            .filter_map(|line| line.ok()?.strip_prefix("Path = ").map(String::from))
            .filter(|file| !file.contains('/'))
            .collect::<Vec<_>>();

        let has_root_within = root_dirs.len() == 1 && *root_dirs[0] == *name;
        let extract_dir = if has_root_within {
            archive.parent().to_path_buf()
        } else {
            Self::get_conversion_root_dir(archive)
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
    archive: ArchivePath,
}

impl ExtractionJob {
    /// Run this job.
    fn run(self) -> Result<TempDirGuard, Exn<ErrorMessage>> {
        let err = || {
            let path = self.archive.display();
            ErrorMessage::new(format!("Failed to extract the archive \"{path}\""))
        };

        if !self.archive.is_file() {
            let exn = ErrorMessage::new("Archive disappeared").raise();
            bail!(exn.raise(err()))
        }

        let extract_dir = ArchiveJob::get_conversion_root_dir(&self.archive);

        if extract_dir.exists() {
            let dir = extract_dir.display();
            let msg = ErrorMessage::new(format!("Extract directory already exists at \"{dir}\""));
            bail!(Exn::new(msg).raise(err()))
        }

        let guard = TempDirGuard::new(extract_dir.clone());
        fs::create_dir_all(&extract_dir)
            .or_raise(|| {
                let extract_dir = extract_dir.display();
                let msg = format!("Could not create the target directory at \"{extract_dir}\"");
                ErrorMessage::new(msg)
            })
            .or_raise(err)?;
        spawn::extract_zip(&self.archive, &extract_dir)
            .and_then(ManagedChild::wait)
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
    /// The extension to use for the Zip file.
    extension: ZipExtension,
}

impl CompressionJob {
    /// Run this job.
    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        let err = || {
            let root = self.root.display();
            ErrorMessage::new(format!("Failed to compress the directory \"{root}\""))
        };

        let zip_path = self.get_zip_path();
        let file = File::create(zip_path).or_raise(err)?;

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        for entry in WalkDir::new(&self.root) {
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
                let bytes = fs::read(entry).or_raise(err)?;
                zipper.write_all(&bytes).or_raise(err)?;
            } else if !inner_path.is_empty() {
                zipper.add_directory(inner_path, options).or_raise(err)?;
            }
        }

        zipper.finish().or_raise(err)?;
        Ok(())
    }

    /// Create the file name path for the newly compressed Zip archive.
    fn get_zip_path(&self) -> PathBuf {
        let dir = self
            .root
            .parent()
            .expect("root is a temporary directory, so it has a parent");
        let name = self
            .root
            .file_stem()
            .expect("root is a temporary directory with a name")
            .to_str()
            .expect("our file paths are utf8 compliant");
        let zip_ext = self.extension.ext();

        let image_ext = self.target.ext();
        dir.join(format!("{name}.{image_ext}.{zip_ext}"))
    }
}

/// A path that was verified to point to an existing Zip archive.
#[derive(Debug, Clone)]
pub struct ArchivePath {
    /// The path to the archive.
    archive: PathBuf,
    /// The file extension used for the archive.
    extension: ZipExtension,
}

impl ArchivePath {
    /// Checked constructor to verify the path points to an archive.
    ///
    /// This only checks that the directory exists at the time of creation.
    pub fn new(archive: PathBuf) -> Result<Self, Exn<ErrorMessage, PathBuf>> {
        let Some(extension) = archive.extension() else {
            let msg = ErrorMessage::new("File is missing a file extension for archives.");
            let exn = Exn::with_recovery(msg, archive);
            return Err(exn);
        };
        let extension = match extension
            .to_str()
            .expect("file names are utf8 compliant")
            .parse::<ZipExtension>()
        {
            Ok(ext) => ext,
            Err(err) => {
                let exn = err.raise();
                let msg = ErrorMessage::new("Archive has an unsupported extension");
                let exn = exn.raise_with_recovery(msg, archive);
                return Err(exn);
            }
        };

        if !archive.is_file() {
            let path = archive.display();
            let msg = format!("Archive does not exist: \"{path}\"");
            let exn = Exn::with_recovery(ErrorMessage::new(msg), archive);
            return Err(exn);
        }

        if archive.file_name().is_none_or(OsStr::is_empty) {
            let path = archive.display();
            let msg = format!("Archive has empty file name: \"{path}\"");
            let exn = Exn::with_recovery(ErrorMessage::new(msg), archive);
            return Err(exn);
        }

        Ok(Self { archive, extension })
    }

    /// Get the parent directory of this archive.
    pub fn parent(&self) -> &Path {
        self.archive.parent().expect("file has parent")
    }

    /// Get the file name for this archive.
    pub fn name(&self) -> &OsStr {
        self.archive
            .file_stem()
            .expect("archive has name by construction")
    }
}

impl std::ops::Deref for ArchivePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.archive
    }
}

/// Possible file extensions for a Zip archive.
#[derive(Debug, Clone, Copy)]
enum ZipExtension {
    /// Mark Zip archive with `.zip`.
    Zip,
    /// Mark Zip archive with `.cbz`.
    Cbz,
}

impl ZipExtension {
    /// Get the file extension as string.
    pub const fn ext(self) -> &'static str {
        use ZipExtension::*;

        match self {
            Zip => "zip",
            Cbz => "cbz",
        }
    }
}

impl std::str::FromStr for ZipExtension {
    type Err = ErrorMessage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ZipExtension::*;

        match s.to_lowercase().as_str() {
            "zip" => Ok(Zip),
            "cbz" => Ok(Cbz),
            _ => Err(ErrorMessage::new("unsupported archive format")),
        }
    }
}
