use std::collections::VecDeque;
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use exn::{ErrorExt, Exn, OptionExt, ResultExt};
use indicatif::ProgressBar;
use tracing::{debug, error};
use walkdir::WalkDir;

use crate::convert::{
    Configuration,
    image::{ConversionJob, ConversionJobs, Details},
};
use crate::error::NothingToDo;
use crate::{convert::image::ImageFormat, error::ErrorMessage};

/// Represents the job to convert all images within a directory.
///
/// When run, this job creates a mirrored directory of hard links to all original files. Then all
/// images are replaced with the converted image type. This is only intended for regular
/// directories, and does not traverse mount points.
pub struct RecursiveDirJob {
    root: Directory,
    hardlink: RecursiveHardLinkJob,
    conversion: ConversionJobs,
}

impl super::Job for RecursiveDirJob {
    fn path(&self) -> &Path {
        &self.root
    }

    fn run(self, bar: &ProgressBar) -> Result<(), Exn<ErrorMessage>> {
        let Self {
            root,
            hardlink,
            conversion,
        } = self;

        let err = || {
            let root = root.deref();
            ErrorMessage::new(format!(
                "Failed to convert all images recursively within {root:?}"
            ))
        };

        let guard = hardlink.run().or_raise(err)?;
        conversion.run(bar).or_raise(err)?;

        guard.keep();
        Ok(())
    }
}

impl RecursiveDirJob {
    /// Create a new job to convert all images in a directory.
    ///
    /// No files get touched until this job is run.
    pub fn new(
        root: Directory,
        config: Configuration,
    ) -> Result<Result<Self, Exn<NothingToDo>>, Exn<ErrorMessage>> {
        let err = || {
            ErrorMessage::new(format!(
                "Failed to prepare job for recursive image conversion starting at {root:?}"
            ))
        };

        let Configuration {
            target, n_workers, ..
        } = config;

        if Self::already_converted(&root, target).or_raise(err)? {
            let msg = format!("Already converted {root:?}");
            let exn = Exn::new(NothingToDo::new(msg));
            return Ok(Err(exn));
        }

        let copy_root = Self::get_hardlink_dir(&root, config.target).or_raise(err)?;
        let job_queue = Self::images_in_dir(&root)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let rel_path = image_path
                    .strip_prefix(&root)
                    .expect("image path is within root by construction");
                let copy_path = copy_root.join(rel_path);
                match Details::new(&copy_path, format, config) {
                    Ok(Some(task)) => {
                        debug!("create job for {copy_path:?}: {task:?}");
                        Some(Ok(ConversionJob::new(copy_path, task)))
                    }
                    Ok(None) => {
                        debug!("skip conversion for {copy_path:?}");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<VecDeque<_>, _>>()
            .or_raise(err)?;
        if job_queue.is_empty() {
            let msg = format!("No files to convert in {root:?}");
            let exn = Exn::new(NothingToDo::new(msg));
            return Ok(Err(exn));
        }

        let hardlink = RecursiveHardLinkJob {
            root: root.clone(),
            target,
        };
        let conversion = ConversionJobs::new(job_queue, n_workers);
        Ok(Ok(Self {
            root,
            hardlink,
            conversion,
        }))
    }

    /// Builds the path to the mirrored directory with hard links.
    fn get_hardlink_dir(
        root: &Directory,
        target: ImageFormat,
    ) -> Result<PathBuf, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new(format!("Directory has no parent: {root:?}"));

        let parent = root.parent().ok_or_raise(err)?;
        let name = root.file_stem().unwrap().to_string_lossy();
        let new_name = format!("{}-{}", name, target.ext());
        Ok(parent.join(new_name))
    }

    /// Checks if the given archive has already been converted.
    ///
    /// A converted archive either already holds the correct image format suffix in its name, or
    /// there exists another archive with the same name and that suffix in the same directory.
    fn already_converted(root: &Directory, target: ImageFormat) -> Result<bool, Exn<ErrorMessage>> {
        let err =
            || ErrorMessage::new("Could not check if this directory has been converted before");

        let converted_path = Self::get_hardlink_dir(root, target).or_raise(err)?;

        let conversion_ending = format!("-{}", target.ext());
        let is_converted_dir = root.to_str().unwrap().ends_with(&conversion_ending);
        let has_converted_dir = converted_path.try_exists().or_raise(err)?;

        Ok(is_converted_dir || has_converted_dir)
    }

    /// Collects all images and their file type found recursively in a directory.
    fn images_in_dir(root: &Directory) -> Result<Vec<(PathBuf, ImageFormat)>, Exn<ErrorMessage>> {
        let err = || {
            ErrorMessage::new(format!(
                "Error when looking for images in directory {root:?}"
            ))
        };

        WalkDir::new(root)
            .into_iter()
            .filter_map(|entry| {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(inner) => {
                        let outer = err();
                        return Some(Err(inner.raise().raise(outer)));
                    }
                };
                let file = entry.path().to_path_buf();
                let ext = file.extension()?.to_string_lossy().to_lowercase();

                use ImageFormat::*;
                let file = match ext.as_str() {
                    "jpg" => Some((file, Jpeg)),
                    "jpeg" => Some((file, Jpeg)),
                    "png" => Some((file, Png)),
                    "avif" => Some((file, Avif)),
                    "jxl" => Some((file, Jxl)),
                    "webp" => Some((file, Webp)),
                    _ => None,
                };
                Ok(file).transpose()
            })
            .collect()
    }
}

/// Represents the job to create a mirror directory of hard links.
///
/// When run, this job creates a new directory named `<original name>-<target extension>`.The
/// directory will create a hard link to all files in the original directory, therefore this is a
/// relatively light-weight operation.
struct RecursiveHardLinkJob {
    root: Directory,
    target: ImageFormat,
}

impl RecursiveHardLinkJob {
    fn run(self) -> Result<TempDirGuard, Exn<ErrorMessage>> {
        let copy_root = RecursiveDirJob::get_hardlink_dir(&self.root, self.target)
            .expect("checked by construction that dir is not root");

        let err = || {
            let root = &self.root;
            ErrorMessage::new(format!(
                "Error while creating hard links from {root:?} to {copy_root:?}"
            ))
        };

        let guard = TempDirGuard::new(copy_root.to_path_buf());

        for entry in WalkDir::new(&self.root).same_file_system(true) {
            let entry = entry.or_raise(err)?;
            let path = entry.path();
            let rel_path = path
                .strip_prefix(&self.root)
                .expect("all files have the root as prefix");
            let copy_path = copy_root.join(rel_path);

            if path.is_file() {
                fs::hard_link(path, &copy_path).or_raise(err)?;
            } else if path.is_dir() {
                fs::create_dir(&copy_path).or_raise(err)?;
            }
        }

        Ok(guard)
    }
}

/// A filesystem path that was verified to point to an existing directory.
#[derive(Debug, Clone)]
pub struct Directory(PathBuf);

impl Directory {
    /// Checked constructor to verify the path points to a directory.
    ///
    /// This only checks that the directory exists at the time of creation.
    pub fn new(path: PathBuf) -> Result<Self, Exn<ErrorMessage, PathBuf>> {
        match path.is_dir() {
            true => Ok(Self(path)),
            false => {
                let msg = format!("Provided path is not a directory: {path:?}");
                let err = Exn::with_recovery(ErrorMessage::new(msg), path);
                Err(err)
            }
        }
    }
}

impl std::ops::Deref for Directory {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::convert::AsRef<Path> for Directory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

/// Deletes the temporary directory when dropped.
///
/// To keep the directory, use [TempDirGuard::keep()].
pub struct TempDirGuard {
    temp_root: Option<PathBuf>,
}

impl TempDirGuard {
    /// Create a guard for a temporary directory. Deletes the directory on drop.
    ///
    /// The directory must not yet exist when the guard is created.
    pub fn new(temp_root: PathBuf) -> Self {
        let temp_root = Some(temp_root);
        Self { temp_root }
    }

    /// Drop the guard without removing the temporary directory.
    pub fn keep(mut self) {
        self.temp_root.take();
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let Some(root) = self.temp_root.take() else {
            return;
        };
        debug!("drop temporary directory {root:?}");
        if root.exists()
            && let Err(e) = fs::remove_dir_all(&root)
        {
            error!("error on deleting directory {root:?}: {e}");
        }
    }
}
