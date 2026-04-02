//! Contains collections of jobs that run many conversions once run.

use std::ops::Not as _;

use exn::Exn;
use tracing::debug;

use crate::convert::archive::ArchiveJob;
use crate::convert::dir::DirectoryJob;
use crate::convert::{Bars, ConversionConfig, Job, JobsBarTitle};
use crate::error::{ErrorMessage, NothingToDo};

/// A trait to specify a collection of [`Job`]'s.
pub trait JobCollection: Sized {
    /// The kind of jobs aggregated here.
    type SubJob: Job;

    /// Wrap a vector of sub jobs as the type.
    fn wrap(jobs: Vec<Self::SubJob>) -> Self;

    /// Unwraps the type again to get back the sub jobs.
    fn unwrap(self) -> Vec<Self::SubJob>;

    /// Iterate over all jobs for inspection.
    fn iter(&self) -> impl Iterator<Item = &Self::SubJob>;

    /// Get the number of jobs stored here.
    fn len(&self) -> usize;

    /// Get the title displayed in the progress bar when run.
    fn bar_title() -> JobsBarTitle;

    /// Create a new job collection given a set of images.
    fn new(
        images: Vec<<Self::SubJob as Job>::Images>,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let items = images
            .into_iter()
            .map(|item| {
                let job = Self::SubJob::new(item, config)?
                    .inspect_err(|NothingToDo { path, reason }| {
                        debug!("{reason}: Skip \"{}\"", path.display());
                    })
                    .ok();
                Ok(job)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, Exn<ErrorMessage>>>()?;

        let wrapped = items.is_empty().not().then_some(Self::wrap(items));
        Ok(wrapped)
    }

    /// Run all jobs in this collection to completion.
    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        let items = self.unwrap();

        let bars = Bars::new(Self::bar_title());
        bars.jobs.set_length(items.len() as u64);

        for item in items {
            bars.println(format!("Converting \"{}\"", item.path().display()));
            item.run(&bars.images)?;
            bars.jobs.inc(1);
        }

        bars.finish();
        Ok(())
    }
}

/// A collection of jobs to convert many archives.
pub struct ArchiveJobCollection(Vec<ArchiveJob>);

impl JobCollection for ArchiveJobCollection {
    type SubJob = ArchiveJob;

    fn wrap(jobs: Vec<Self::SubJob>) -> Self {
        Self(jobs)
    }

    fn unwrap(self) -> Vec<Self::SubJob> {
        self.0
    }

    fn iter(&self) -> impl Iterator<Item = &Self::SubJob> {
        self.0.iter()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn bar_title() -> JobsBarTitle {
        JobsBarTitle::Archives
    }
}

/// A collection of jobs to convert many directories.
pub struct DirectoryJobCollection(Vec<DirectoryJob>);

impl JobCollection for DirectoryJobCollection {
    type SubJob = DirectoryJob;

    fn wrap(jobs: Vec<Self::SubJob>) -> Self {
        Self(jobs)
    }

    fn unwrap(self) -> Vec<Self::SubJob> {
        self.0
    }

    fn iter(&self) -> impl Iterator<Item = &Self::SubJob> {
        self.0.iter()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn bar_title() -> JobsBarTitle {
        JobsBarTitle::Directories
    }
}
