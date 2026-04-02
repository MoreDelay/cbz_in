//! Contains collections of jobs that run many conversions once run.

use std::ops::Not as _;

use exn::Exn;
use tracing::debug;

use crate::convert::{Bars, ConversionConfig, Job};
use crate::error::{ErrorMessage, NothingToDo};

/// A collection of jobs that are run sequentially.
pub struct JobCollection<J: Job>(Vec<J>);

impl<J: Job> JobCollection<J> {
    /// Create a new job collection given a set of images.
    pub fn new(
        images: Vec<<J as Job>::Images>,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let jobs = images
            .into_iter()
            .map(|item| {
                let job = J::new(item, config)?
                    .inspect_err(|NothingToDo { path, reason }| {
                        debug!("{reason}: Skip \"{}\"", path.display());
                    })
                    .ok();
                Ok(job)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, Exn<ErrorMessage>>>()?;

        let wrapped = jobs.is_empty().not().then_some(Self(jobs));
        Ok(wrapped)
    }

    /// Run all jobs in this collection to completion.
    pub fn run(self) -> Result<(), Exn<ErrorMessage>> {
        let Self(jobs) = self;

        let bars = Bars::new(J::title());
        bars.jobs.set_length(jobs.len() as u64);

        for item in jobs {
            bars.println(format!("Converting \"{}\"", item.path().display()));
            item.run(&bars.images)?;
            bars.jobs.inc(1);
        }

        bars.finish();
        Ok(())
    }

    /// Iterate over all jobs.
    pub fn iter(&self) -> impl Iterator<Item = &J> {
        self.0.iter()
    }

    /// Get the count of jobs aggregated here.
    pub const fn len(&self) -> usize {
        self.0.len()
    }
}
