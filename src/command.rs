//! Contains the high level control flow items to execute the program.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::ops::Deref as _;
use std::path::PathBuf;

use exn::{ErrorExt as _, Exn, ResultExt as _};
use tracing::info;

use crate::convert::archive::ArchiveJob;
use crate::convert::collection::{ImageCollection, JobCollection};
use crate::convert::dir::DirectoryJob;
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirectoryImages};
use crate::convert::{ConversionConfig, Job};
use crate::error::ErrorMessage;
use crate::stats::Stats;
use crate::{ConversionTarget, stdout};

/// All collections found in the locations specified by the user.
pub enum FoundCollections {
    /// We looked for images in archives.
    Archives(ImageCollection<ArchiveImages>),
    /// We looked for images in directories.
    Directories(ImageCollection<DirectoryImages>),
}

impl FoundCollections {
    /// Look for images in archives.
    pub fn on_archives(
        paths: impl Iterator<Item = PathBuf>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        Ok(ImageCollection::on_archives(paths)?.map(Self::Archives))
    }

    /// Look for images in directories.
    pub fn on_directories(
        paths: impl Iterator<Item = PathBuf>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        Ok(ImageCollection::on_directories(paths)?.map(Self::Directories))
    }

    /// Filter out all images such that only those remain that are specified in the filter.
    pub fn filter(self, filter: &HashSet<ImageFormat>) -> Option<Self> {
        match self {
            Self::Archives(images) => images.filter(filter).map(Self::Archives),
            Self::Directories(images) => images.filter(filter).map(Self::Directories),
        }
    }

    /// Print the number of images found per image format to stdout.
    pub fn print_stats(&self, verbose: bool) {
        match self {
            Self::Archives(images) => images.print_stats(verbose),
            Self::Directories(images) => images.print_stats(verbose),
        }
    }

    /// Run conversion on the found images.
    pub fn convert(
        self,
        target: ConversionTarget,
        n_workers: NonZeroUsize,
    ) -> Result<(), Exn<ErrorMessage>> {
        let config = ConversionConfig { target, n_workers };

        let Some(run) = RunConversion::new(self, config)? else {
            stdout("Nothing to do");
            return Ok(());
        };

        run.test()?;
        run.run()
    }
}

/// Print out statistics per image format.
pub fn print_stats_per_format(stats: &Stats) {
    let mut counts = Vec::from_iter(stats.inner.clone());
    counts.sort_unstable_by_key(|(f, _)| f.ext());
    for (format, count) in &counts {
        let format = format.ext();
        stdout(format!("{format}: {count}"));
    }
}

/// Print out the total number of images found.
pub fn print_stats_total(stats: &Stats) {
    let total: usize = stats.inner.values().sum();
    stdout(format!("total: {total}"));
}

/// Helper object to run conversion
enum RunConversion {
    /// We run conversion on archives.
    Archives(JobCollection<ArchiveJob>),
    /// We run conversion on directories.
    Directories(JobCollection<DirectoryJob>),
}

impl RunConversion {
    /// Prepare conversion jobs.
    fn new(
        found: FoundCollections,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let out = match found {
            FoundCollections::Archives(items) => {
                JobCollection::new(items, config)?.map(Self::Archives)
            }
            FoundCollections::Directories(items) => {
                JobCollection::new(items, config)?.map(Self::Directories)
            }
        };

        Ok(out)
    }

    /// Run the conversion for real.
    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        match self {
            Self::Archives(coll) => coll.run()?,
            Self::Directories(coll) => coll.run()?,
        }
        Ok(())
    }

    /// Check if we can run this job, and print out statistics.
    fn test(&self) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Doing dry run");

        self.check_tools().or_raise(err)?;
        self.print_statistics();

        let paths: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.iter().map(|j| j.path().deref()),
            Self::Directories(jobs) => &mut jobs.iter().map(|j| j.path().deref()),
        };

        for path in paths {
            info!("Got files to convert for \"{}\"", path.display());
        }

        Ok(())
    }

    /// Check if all tools needed for this job are actually available.
    fn check_tools(&self) -> Result<(), Exn<ErrorMessage>> {
        let iter: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.iter().flat_map(Job::iter),
            Self::Directories(jobs) => &mut jobs.iter().flat_map(Job::iter),
        };
        let required_tools = iter
            .flat_map(|job| job.plan().required_tools())
            .collect::<HashSet<_>>();
        let missing_tools = required_tools
            .into_iter()
            .filter_map(|tool| match tool.available() {
                Ok(true) => None,
                Ok(false) => Some(Ok(tool.name())),
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !missing_tools.is_empty() {
            let mut missing_tools = missing_tools;
            missing_tools.sort_unstable();
            let tools = missing_tools.join(", ");
            let msg = format!("Missing tools: {tools}");
            let exn = ErrorMessage::new(msg).raise();
            return Err(exn);
        }
        Ok(())
    }

    /// Print out statistics on how many images would get converted by this job.
    fn print_statistics(&self) {
        let collections = match self {
            Self::Archives(jobs) => jobs.len(),
            Self::Directories(jobs) => jobs.len(),
        };

        let images: usize = match self {
            Self::Archives(jobs) => jobs.iter().map(Job::count).sum(),
            Self::Directories(jobs) => jobs.iter().map(Job::count).sum(),
        };

        let coll_type = match self {
            Self::Archives(_) => "archives",
            Self::Directories(_) => "directories",
        };

        stdout(format!(
            "Found {collections} {coll_type}, with a total of {images} images to convert"
        ));
    }
}
