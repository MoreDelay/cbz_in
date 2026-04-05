//! Contains collections of jobs that run many conversions once run.

use std::collections::HashSet;
use std::ops::Not as _;

use exn::Exn;
use tracing::debug;

use crate::command::{print_stats_per_format, print_stats_total};
use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirectoryImages, Images};
use crate::convert::{Bars, ConversionConfig, ImagesJob};
use crate::error::{ErrorMessage, NothingToDo};
use crate::stats::Stats;
use crate::stdout;

/// A container for many image collections found in a search run.
pub struct ImageCollection<I: Images>(Vec<I>);

impl ImageCollection<ArchiveImages> {
    /// Look for images in archives.
    pub fn in_archives(
        paths: impl Iterator<Item = ArchivePath>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        Self::new(paths)
    }
}

impl ImageCollection<DirectoryImages> {
    /// Look for images in directories.
    pub fn in_directories(
        paths: impl Iterator<Item = Directory>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        Self::new(paths)
    }
}

impl<I: Images> ImageCollection<I> {
    /// Generic constructor that relies on [`Images`] behavior to search for images.
    fn new(paths: impl Iterator<Item = I::Path>) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let found = paths
            .into_iter()
            .map(|path| {
                let images = I::search(path)?
                    .inspect_err(|dir| {
                        let name = I::fs_root().singular();
                        debug!("{name} has no images: \"{}\"", dir.display());
                    })
                    .ok();
                Ok(images)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, Exn<_>>>()?;

        Ok(found.is_empty().not().then_some(Self(found)))
    }

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
        let roots = I::fs_root().plural().to_ascii_lowercase();
        stdout(format!("Searched {count} {roots}:"));

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
pub struct JobCollection<J: ImagesJob>(Vec<J>);

impl<J: ImagesJob> JobCollection<J> {
    /// Create a new job collection given a set of images.
    pub fn new(
        images: ImageCollection<<J as ImagesJob>::Images>,
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
