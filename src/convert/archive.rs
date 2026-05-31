//! Contains everything related to handling zip archives.

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead as _, Write as _};
use std::path::{Path, PathBuf};

use exn::{ErrorExt as _, Exn, ResultExt as _, bail};
use indicatif::ProgressBar;
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use super::ImagesJob;
use crate::ConversionTarget;
use crate::convert::dir::TempDirGuard;
use crate::convert::image::{ConversionJob, ConversionJobs};
use crate::convert::search::{ArchiveImages, Images};
use crate::convert::{ConversionConfig, JobPath};
use crate::error::{Msg, NothingToDo, NothingToDoReason};
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

impl ImagesJob for ArchiveJob {
    /// The image collection this job works on
    type Images = ArchiveImages;

    fn new(
        images: Self::Images,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo<<Self::Images as Images>::Path>>, Exn<Msg<Self>>> {
        Self::new_internal(images, config)
    }

    fn path(&self) -> &JobPath<Self> {
        &self.archive
    }

    fn iter(&self) -> impl Iterator<Item = &ConversionJob> {
        self.conversion.iter()
    }

    fn count(&self) -> usize {
        self.conversion.len()
    }

    /// Run this job.
    fn run(self, bar: Option<&ProgressBar>) -> Result<(), Exn<Msg<Self>>> {
        let Self {
            archive,
            extraction,
            conversion,
            compression,
        } = self;

        let err = || {
            let archive = archive.display();
            Msg::new(format!("Converting images in archive \"{archive}\""))
        };

        if let Some(bar) = bar {
            bar.reset(); // start time tracking for this job here
            bar.set_length(conversion.len() as u64);
        }
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
    fn new_internal(
        archive: ArchiveImages,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo<ArchivePath>>, Exn<Msg<Self>>> {
        let ArchiveImages { root, images } = archive;
        let archive = root;

        let err = || {
            let archive = archive.display();
            let msg = format!("Preparing job for archive conversion \"{archive}\"");
            Msg::new(msg)
        };

        let target = config.target;

        if Self::already_converted(&archive, target).or_raise(err)? {
            let reason = NothingToDoReason::AlreadyConverted;
            let path = archive;
            return Ok(Err(NothingToDo { path, reason }));
        }

        let root_dir = Self::get_extraction_root_dir(&archive).or_raise(err)?;

        let extraction = ExtractionJob {
            archive: archive.clone(),
        };
        let Some(conversion) = ConversionJobs::new(images, &root_dir, config).or_raise(err)? else {
            let reason = NothingToDoReason::NothingToConvert;
            let path = archive;
            return Ok(Err(NothingToDo { path, reason }));
        };

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
        let name = archive.file_stem();
        dir.join(name)
    }

    /// Checks if the given archive has already been converted.
    ///
    /// A converted archive either already holds the correct image format suffix in its name, or
    /// there exists another archive with the same name and that suffix in the same directory.
    pub fn already_converted(
        path: &ArchivePath,
        target: ConversionTarget,
    ) -> Result<bool, Exn<Msg<Self>>> {
        let err = || {
            let path = path.display();
            Msg::new(format!(
                "Checking if archive has been converted before: \"{path}\""
            ))
        };

        let conversion_ending = format!(".{}.{}", target.format().ext(), path.extension.ext());

        let dir = path.parent();
        let name = path.file_stem();
        let zip_path = dir.join(format!("{name}{conversion_ending}"));

        let is_converted_archive = path.file_name().ends_with(&conversion_ending);
        let has_converted_archive = zip_path.try_exists().or_raise(err)?;

        Ok(is_converted_archive || has_converted_archive)
    }

    /// Builds the path that should be provided for 7z such that it aligns with our expectations.
    ///
    /// When the archive gets compressed later, it should include a single top-level directory with
    /// the same file name as the archive itself, without extensions. If the archive already has
    /// this directory, then it can be extracted directly. Otherwise the root directory must be
    /// created first, and the contents are extracted inside of that.
    fn get_extraction_root_dir(archive: &ArchivePath) -> Result<PathBuf, Exn<Msg<Self>>> {
        let err = || {
            let archive = archive.display();
            let msg = format!("Determining extraction dir for \"{archive}\"");
            Msg::new(msg)
        };

        let name = archive.file_stem();
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
    fn run(self) -> Result<TempDirGuard, Exn<Msg<Self>>> {
        let err = || {
            let path = self.archive.display();
            Msg::new(format!("Extracting the archive \"{path}\""))
        };

        if !self.archive.is_file() {
            let exn = Msg::no_tag("Archive disappeared").raise();
            bail!(exn.raise(err()))
        }

        let extract_dir = ArchiveJob::get_conversion_root_dir(&self.archive);

        if extract_dir.exists() {
            let dir = extract_dir.display();
            let msg = Msg::no_tag(format!("Extract directory already exists at \"{dir}\""));
            bail!(Exn::new(msg).raise(err()))
        }

        let guard = TempDirGuard::new(extract_dir.clone());
        fs::create_dir_all(&extract_dir)
            .or_raise(|| {
                let extract_dir = extract_dir.display();
                let msg = format!("Could not create the target directory at \"{extract_dir}\"");
                Msg::no_tag(msg)
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
    target: ConversionTarget,
    /// The extension to use for the Zip file.
    extension: ZipExtension,
}

impl CompressionJob {
    /// Run this job.
    fn run(self) -> Result<(), Exn<Msg<Self>>> {
        let err = || {
            let root = self.root.display();
            Msg::new(format!("Compressing directory \"{root}\""))
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

        let image_ext = self.target.format().ext();
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
    pub fn new(archive: PathBuf) -> Result<Self, (PathBuf, Exn<Msg<Self>>)> {
        if !archive.is_file() {
            let msg = Msg::new("This is not a file");
            return Err((archive, msg.raise()));
        }

        let Some(extension) = archive.extension() else {
            let msg = Msg::new("File is missing a file extension for archives.");
            return Err((archive, msg.raise()));
        };
        let extension = match extension
            .to_str()
            .expect("file names are utf8 compliant")
            .parse::<ZipExtension>()
        {
            Ok(ext) => ext,
            Err(err) => {
                let exn = err.raise();
                let msg = Msg::new("File has an unsupported extension");
                return Err((archive, exn.raise(msg)));
            }
        };

        if archive.file_name().is_none_or(OsStr::is_empty) {
            let msg = Msg::new("Archive has empty file name");
            return Err((archive, msg.raise()));
        }

        Ok(Self { archive, extension })
    }

    /// Get the parent directory of this archive.
    #[must_use]
    pub fn parent(&self) -> &Path {
        self.archive.parent().expect("file has parent")
    }

    /// Get the file name for this archive.
    #[must_use]
    pub fn file_stem(&self) -> &str {
        self.archive
            .file_stem()
            .expect("archive has name by construction")
            .to_str()
            .expect("files should be unicode")
    }

    /// Get the full file name including extension for this archive.
    #[must_use]
    pub fn file_name(&self) -> &str {
        self.archive
            .file_name()
            .expect("archive has name by construction")
            .to_str()
            .expect("files should be unicode")
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
    type Err = Msg<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use ZipExtension::*;

        match s.to_lowercase().as_str() {
            "zip" => Ok(Zip),
            "cbz" => Ok(Cbz),
            _ => Err(Msg::new("Unsupported archive format")),
        }
    }
}
