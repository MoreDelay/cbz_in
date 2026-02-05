use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};

use exn::{ErrorExt, Exn, OptionExt, ResultExt};
use indicatif::ProgressBar;
use tracing::{debug, error};
use walkdir::WalkDir;

use crate::convert::{
    Configuration,
    image::{ConversionJob, ConversionJobDetails, ConversionJobs},
};
use crate::error::NothingToDo;
use crate::{convert::image::ImageFormat, error::ErrorMessage};

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
            ErrorMessage::new(format!(
                "Failed to convert all images recursively in {root:?}"
            ))
        };

        let guard = hardlink.run().or_raise(err)?;
        conversion.run(bar).or_raise(err)?;

        guard.keep();
        Ok(())
    }
}

impl RecursiveDirJob {
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

        let copy_root = Self::get_hardlink_dir(&root, config.target).or_raise(err)?;
        let job_queue = Self::images_in_dir(&root)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let rel_path = image_path
                    .strip_prefix(&root)
                    .expect("image path is within root by construction");
                let copy_path = copy_root.join(rel_path);
                match ConversionJobDetails::new(&copy_path, format, config) {
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
}

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

#[derive(Debug, Clone)]
pub struct Directory(PathBuf);

impl Directory {
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

/// Deletes the temporary directory when dropped
///
/// To keep the directory, use `guard.keep()`
pub struct TempDirGuard {
    temp_root: Option<PathBuf>,
}

impl TempDirGuard {
    pub fn new(temp_root: PathBuf) -> Self {
        let temp_root = Some(temp_root);
        Self { temp_root }
    }

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
