use std::ops::Deref;

use exn::{Exn, ResultExt};
use tracing::{debug, info};

use crate::{
    convert::{
        Configuration, JobCollection,
        archive::ArchivePath,
        dir::{Directory, RecursiveDirJob},
    },
    error::ErrorMessage,
};

use super::archive::ArchiveJob;

/// Represents a collection of [ArchiveJob]'s, which are all performed in one operation.
pub struct ArchiveJobs(Vec<ArchiveJob>);

impl JobCollection for ArchiveJobs {
    type Single = ArchiveJob;

    fn jobs(&self) -> usize {
        self.0.len()
    }
}

impl ArchiveJobs {
    /// The constructed [ArchiveJobs] will contain only a single [ArchiveJob].
    pub fn single(
        archive: ArchivePath,
        config: Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        match ArchiveJob::new(archive, config)? {
            Ok(job) => Ok(Some(Self(vec![job]))),
            Err(nothing_to_do) => {
                info!("{nothing_to_do}");
                Ok(None)
            }
        }
    }

    /// Create an [ArchiveJob] for all archives found in the provided root directory.
    pub fn collect(root: Directory, config: Configuration) -> Result<Self, Exn<ErrorMessage>> {
        let err = || {
            ErrorMessage::new(format!(
                "Error while looking for archives needing conversion from root {root:?}"
            ))
        };

        let jobs = root
            .read_dir()
            .or_raise(err)?
            .filter_map(|dir_entry| {
                let path = match dir_entry.or_raise(err) {
                    Ok(dir_entry) => dir_entry.path(),
                    Err(e) => return Some(Err(e)),
                };
                let archive = match ArchivePath::new(path) {
                    Ok(archive) => archive,
                    Err(exn) => {
                        let (path, exn) = exn.recover();
                        debug!("skipping {path:?}: {exn:?}");
                        return None;
                    }
                };

                info!("Checking {:?}", archive.deref());
                match ArchiveJob::new(archive, config) {
                    Ok(Ok(job)) => Some(Ok(job)),
                    Ok(Err(nothing_to_do)) => {
                        info!("{nothing_to_do}");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<_>, Exn<ErrorMessage>>>()?;
        Ok(Self(jobs))
    }

    /// Combine all [ArchiveJob]'s to wrap them up in a new collection.
    pub fn new(iter: impl IntoIterator<Item = ArchiveJob>) -> Option<Self> {
        let jobs = iter.into_iter().collect::<Vec<_>>();
        match jobs.is_empty() {
            true => None,
            false => Some(Self(jobs)),
        }
    }
}

impl Extend<ArchiveJob> for ArchiveJobs {
    fn extend<T: IntoIterator<Item = ArchiveJob>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}

impl IntoIterator for ArchiveJobs {
    type Item = ArchiveJob;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Represents a collection of [RecursiveDirJob]'s, which are all performed in one operation.
pub struct RecursiveDirJobs(Vec<RecursiveDirJob>);

impl JobCollection for RecursiveDirJobs {
    type Single = RecursiveDirJob;

    fn jobs(&self) -> usize {
        self.0.len()
    }
}

impl RecursiveDirJobs {
    /// The constructed [RecursiveDirJobs] will contain only a single [RecursiveDirJob].
    pub fn single(
        dir: Directory,
        config: Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        match RecursiveDirJob::new(dir, config)? {
            Ok(job) => Ok(Some(Self(vec![job]))),
            Err(nothing_to_do) => {
                info!("{nothing_to_do}");
                Ok(None)
            }
        }
    }

    /// Combine all [RecursiveDirJob]'s to wrap them up in a new collection.
    pub fn new(iter: impl IntoIterator<Item = RecursiveDirJob>) -> Option<Self> {
        let jobs = iter.into_iter().collect::<Vec<_>>();
        match jobs.is_empty() {
            true => None,
            false => Some(Self(jobs)),
        }
    }
}

impl Extend<RecursiveDirJob> for RecursiveDirJobs {
    fn extend<T: IntoIterator<Item = RecursiveDirJob>>(&mut self, iter: T) {
        self.0.extend(iter);
    }
}

impl IntoIterator for RecursiveDirJobs {
    type Item = RecursiveDirJob;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
