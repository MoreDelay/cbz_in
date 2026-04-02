//! Contains collections of jobs that run many conversions once run.

use std::collections::HashSet;
use std::ops::Not as _;
use std::path::PathBuf;

use exn::{ErrorExt as _, Exn};
use tracing::debug;

use crate::command::{print_stats_per_format, print_stats_total};
use crate::convert::dir::Directory;
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirectoryImages, Images};
use crate::convert::{Bars, ConversionConfig, DirOrArchive, Job};
use crate::error::{ErrorMessage, NothingToDo};
use crate::stats::Stats;
use crate::stdout;

/// A container for many image collections found in a search run.
pub struct ImageCollection<I: Images>(Vec<I>);

impl ImageCollection<ArchiveImages> {
    /// Look for images in archives.
    pub fn on_archives(
        paths: impl Iterator<Item = PathBuf>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let found = paths
            .into_iter()
            .map(|path| DirOrArchive::check(path)?.archive_iter())
            .collect::<Result<Vec<_>, Exn<_>>>()?
            .into_iter()
            .flatten()
            .map(|path| {
                let images = ArchiveImages::new(path)?
                    .inspect_err(|path| {
                        debug!("Archive has no images: \"{}\"", path.display());
                    })
                    .ok();
                Ok(images)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, Exn<_>>>()?;

        Ok(found.is_empty().not().then_some(Self(found)))
    }
}

impl ImageCollection<DirectoryImages> {
    /// Look for images in directories.
    pub fn on_directories(
        paths: impl Iterator<Item = PathBuf>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let found = paths
            .into_iter()
            .map(|path| {
                let dir = Directory::new(path)?.map_err(|path| {
                    let msg = format!("Path is not a directory: \"{}\"", path.display());
                    ErrorMessage::new(msg).raise()
                })?;

                let images = DirectoryImages::search_recursive(dir)?
                    .inspect_err(|dir| {
                        debug!("Directory has no images: \"{}\"", dir.display());
                    })
                    .ok();
                Ok(images)
            })
            .collect::<Result<Vec<_>, Exn<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(found.is_empty().not().then_some(Self(found)))
    }
}

impl<I: Images> ImageCollection<I> {
    /// Filter out all images such that only those remain that are specified in the filter.
    pub fn filter(self, filter: &HashSet<ImageFormat>) -> Option<Self> {
        let images = self
            .0
            .into_iter()
            .filter_map(|single| {
                single
                    .filter(filter)
                    .inspect_err(|path| {
                        let name = I::fs_root().singular();
                        debug!(
                            "{name} has no images left after filtering: \"{}\"",
                            path.display()
                        );
                    })
                    .ok()
            })
            .collect::<Vec<_>>();
        images.is_empty().not().then_some(Self(images))
    }

    /// Print the number of images found per image format to stdout.
    pub fn print_stats(&self, verbose: bool) {
        let mut all_stats = Stats::new();

        let count = self.0.len();
        stdout(format!("Searched {count} archives:"));

        for single in &self.0 {
            let stats = Stats::compute(single.infos());
            all_stats.combine(&stats);

            if verbose {
                let header = format!("\"{}\":", single.path().display());
                stdout(header);
                stats.print_per_format();
                stdout("---");
                stats.print_total();
                stdout(""); // new line
            }
        }

        stdout("");
        print_stats_per_format(&all_stats);
        stdout("---");
        print_stats_total(&all_stats);
    }
}

/// A collection of jobs that are run sequentially.
pub struct JobCollection<J: Job>(Vec<J>);

impl<J: Job> JobCollection<J> {
    /// Create a new job collection given a set of images.
    pub fn new(
        images: ImageCollection<<J as Job>::Images>,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let jobs = images
            .0
            .into_iter()
            .map(|images| {
                let job = J::new(images, config)?
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

        let root = <J::Images as Images>::fs_root();
        let bars = Bars::new(root);
        bars.jobs.set_length(jobs.len() as u64);

        for job in jobs {
            bars.println(format!("Converting \"{}\"", job.path().display()));
            job.run(&bars.images)?;
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
