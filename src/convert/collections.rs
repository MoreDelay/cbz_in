//! Contains jobs on collections of images, such as archives or directories

use exn::{Exn, ResultExt as _};
use tracing::debug;

use crate::convert::archive::{ArchiveJob, ArchivePath};
use crate::convert::dir::{Directory, RecursiveDirJob};
use crate::convert::search::{ArchiveImages, DirImages};
use crate::convert::{ConversionConfig, JobCollection};
use crate::error::ErrorMessage;

/// Represents a collection of [`ArchiveJob`]'s, which are all performed in one operation.
pub struct ArchiveJobs(Vec<ArchiveJob>);

impl JobCollection for ArchiveJobs {
    type Single = ArchiveJob;

    fn jobs(&self) -> impl Iterator<Item = &Self::Single> {
        self.0.iter()
    }
}

impl ArchiveJobs {
    /// The constructed [`ArchiveJobs`] will contain only a single [`ArchiveJob`].
    pub fn single(
        archive: ArchivePath,
        config: &ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        Ok(Self::single_internal(archive, config)?.map(|job| Self(vec![job])))
    }

    /// Create an [`ArchiveJob`] for all archives found in the provided root directory.
    pub fn collect(
        root: &Directory,
        config: &ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || {
            let root = root.display();
            ErrorMessage::new(format!(
                "Error while looking for archives needing conversion in directory \"{root}\""
            ))
        };

        let jobs = root
            .read_dir()
            .or_raise(err)?
            .map(|dir_entry| {
                let path = dir_entry.or_raise(err)?.path();
                let archive = match ArchivePath::new(path) {
                    Ok(archive) => archive,
                    Err(exn) => {
                        debug!("skipping: {exn:?}");
                        return Ok(None);
                    }
                };
                Self::single_internal(archive, config)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::aggregate(jobs))
    }

    /// Combine all [`ArchiveJob`]'s to wrap them up in a new collection.
    pub fn aggregate(iter: impl IntoIterator<Item = ArchiveJob>) -> Option<Self> {
        let jobs = iter.into_iter().collect::<Vec<_>>();
        if jobs.is_empty() {
            return None;
        }
        Some(Self(jobs))
    }

    /// Internal constructor for a single [`ArchiveJobs`].
    fn single_internal(
        archive: ArchivePath,
        config: &ConversionConfig,
    ) -> Result<Option<ArchiveJob>, Exn<ErrorMessage>> {
        let archive = ArchiveImages::new(archive)?;
        let Some(archive) = archive else {
            return Ok(None);
        };

        match ArchiveJob::new(archive, config)? {
            Ok(job) => Ok(Some(job)),
            Err(nothing_to_do) => {
                debug!("{nothing_to_do}");
                Ok(None)
            }
        }
    }
}

impl IntoIterator for ArchiveJobs {
    type Item = ArchiveJob;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Represents a collection of [`RecursiveDirJob`]'s, which are all performed in one operation.
pub struct RecursiveDirJobs(Vec<RecursiveDirJob>);

impl JobCollection for RecursiveDirJobs {
    type Single = RecursiveDirJob;

    fn jobs(&self) -> impl Iterator<Item = &Self::Single> {
        self.0.iter()
    }
}

impl RecursiveDirJobs {
    /// The constructed [`RecursiveDirJobs`] will contain only a single [`RecursiveDirJob`].
    pub fn single(
        dir: Directory,
        config: &ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let dir = DirImages::search_recursive(dir)?;
        let Some(dir) = dir else {
            return Ok(None);
        };

        match RecursiveDirJob::new(dir, config)? {
            Ok(job) => Ok(Some(Self(vec![job]))),
            Err(nothing_to_do) => {
                debug!("{nothing_to_do}");
                Ok(None)
            }
        }
    }

    /// Combine all [`RecursiveDirJob`]'s to wrap them up in a new collection.
    pub fn aggregate(iter: impl IntoIterator<Item = RecursiveDirJob>) -> Option<Self> {
        let jobs = iter.into_iter().collect::<Vec<_>>();
        if jobs.is_empty() {
            return None;
        }
        Some(Self(jobs))
    }
}

impl IntoIterator for RecursiveDirJobs {
    type Item = RecursiveDirJob;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
