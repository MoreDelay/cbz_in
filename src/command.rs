//! Contains the main, high-level job which performs the command chosen by the user

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::thread;

use exn::{Exn, ResultExt as _, bail};
use tracing::{debug, info};

use crate::convert::archive::ArchivePath;
use crate::convert::collections::{ArchiveJobs, RecursiveDirJobs};
use crate::convert::dir::Directory;
use crate::convert::search::{ArchiveImages, DirImages, ImageInfo};
use crate::convert::{Bars, ConversionConfig, Job, JobCollection as _, JobsBarTitle};
use crate::error::ErrorMessage;
use crate::{Args, stdout};

/// The top-level task of the application, as determined by user arguments.
pub struct MainJob(MainJobImpl);

impl MainJob {
    /// Execute the main job on archives.
    ///
    /// Convert all found images according to `config`. If `config` is `None`, then we only collect
    /// statistics.
    pub fn on_archives(
        paths: VecDeque<PathBuf>,
        config: &MainJobConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        MainJobImpl::on_archives(paths, config).map(|opt| opt.map(Self))
    }

    /// Execute the main job on directories.
    ///
    /// Convert all found images according to `config`. If `config` is `None`, then we only collect
    /// statistics.
    pub fn on_directories(
        paths: VecDeque<PathBuf>,
        config: &MainJobConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        MainJobImpl::on_directories(paths, config).map(|opt| opt.map(Self))
    }

    /// Run this job.
    pub fn run(self, dry_run: bool) -> Result<(), Exn<ErrorMessage>> {
        self.0.run(dry_run)
    }
}

/// The different options of top-level tasks.
enum MainJobImpl {
    /// We print statistics.
    Stats(StatsJob),
    /// We convert images.
    Convert(ConvertJob),
}

impl MainJobImpl {
    /// Create the main job on archives.
    fn on_archives(
        paths: VecDeque<PathBuf>,
        config: &MainJobConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        use MainJobConfig::*;

        let job = match config {
            Stats => StatsJob::on_archives(paths).map(Some)?.map(Self::Stats),
            Convert(config) => ConvertJob::on_archives(paths, config)?.map(Self::Convert),
        };
        Ok(job)
    }

    /// Create the main job on directories.
    fn on_directories(
        paths: VecDeque<PathBuf>,
        config: &MainJobConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        use MainJobConfig::*;

        let job = match config {
            Stats => StatsJob::on_directories(paths).map(Some)?.map(Self::Stats),
            Convert(config) => ConvertJob::on_directories(paths, config)?.map(Self::Convert),
        };
        Ok(job)
    }

    /// Run this job.
    pub fn run(self, dry_run: bool) -> Result<(), Exn<ErrorMessage>> {
        match self {
            Self::Stats(job) => job.run(),
            Self::Convert(job) => job.run(dry_run)?,
        }
        Ok(())
    }
}

/// Our job is to print statistics about the images we find.
pub enum StatsJob {
    /// We work on archives.
    Archives(Vec<ArchiveImages>),
    /// We work on directories.
    Directories(Vec<DirImages>),
}

impl StatsJob {
    /// Create a [`StatsJob::Archives`].
    fn on_archives(paths: VecDeque<PathBuf>) -> Result<Self, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path)? {
                Ok(root) => return Self::for_archives_in_dir(&root),
                Err(exn) => exn.recover(),
            };

            let (path, archive_exn) = match ArchivePath::new(path) {
                Ok(archive) => return Self::for_single_archive(archive).map(|a| vec![a]),
                Err(exn) => exn.recover(),
            };

            let path = path.display();
            let msg = format!("Neither an archive nor a directory: \"{path}\"");
            let exn = Exn::raise_all(ErrorMessage::new(msg), [dir_exn, archive_exn]);
            Err(exn)
        };

        let err = || ErrorMessage::new("Failed to collect all archives");

        stdout("Looking for images to convert in archives...");

        let archives = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .collect();
        Ok(Self::Archives(archives))
    }

    /// Create a [`StatsJob::Directories`].
    fn on_directories(paths: VecDeque<PathBuf>) -> Result<Self, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        let collect_single = |path| {
            let root = Directory::new(path)?.map_err(Exn::discard_recovery)?;
            DirImages::search_recursive(root)
        };

        stdout("Looking for images to convert in directories...");

        let images = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?;
        Ok(Self::Directories(images))
    }

    /// Run this job.
    fn run(self) {
        let images: &mut dyn Iterator<Item = ImageInfo> = match self {
            Self::Archives(images) => {
                let count = images.len();
                stdout(format!("Searched {count} archives:"));
                &mut images.into_iter().flatten()
            }
            Self::Directories(images) => {
                let count = images.len();
                stdout(format!("Searched {count} directories:"));
                &mut images.into_iter().flatten()
            }
        };

        let counts = images.fold(HashMap::new(), |mut counts, info| {
            counts
                .entry(info.format())
                .and_modify(|v| *v += 1)
                .or_insert(1);
            counts
        });
        let mut counts = Vec::from_iter(counts);
        counts.sort_unstable_by_key(|(f, _)| f.ext());
        for (format, count) in &counts {
            let format = format.ext();
            stdout(format!("{format}: {count}"));
        }
        let total: usize = counts.iter().map(|(_, c)| c).sum();
        stdout("---");
        stdout(format!("total: {total}"));
    }

    /// Create an [`ArchiveJobs`] for a single archive.
    fn for_single_archive(archive: ArchivePath) -> Result<ArchiveImages, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to search images in a single archive");

        info!("Checking archive \"{}\"", archive.display());
        ArchiveImages::new(archive).or_raise(err)
    }

    /// Create an [`ArchiveJobs`] for all archives in a directory.
    fn for_archives_in_dir(root: &Directory) -> Result<Vec<ArchiveImages>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to search images in all archives in a directory");

        info!("Checking archives directory \"{}\"", root.display());
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
                        debug!("skipping: {exn:?}");
                        return None;
                    }
                };
                Some(ArchiveImages::new(archive))
            })
            .collect::<Result<Vec<_>, _>>();
        jobs.or_raise(err)
    }
}

/// Our job is to convert images in archives or directories.
enum ConvertJob {
    /// We work on archives.
    Archives(ArchiveJobs),
    /// We work on directories.
    Directories(RecursiveDirJobs),
}

impl ConvertJob {
    /// Create a [`ConvertJob::Archives`].
    fn on_archives(
        paths: VecDeque<PathBuf>,
        config: &ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path)? {
                Ok(root) => return Self::for_archives_in_dir(&root, config),
                Err(exn) => exn.recover(),
            };

            let (path, archive_exn) = match ArchivePath::new(path) {
                Ok(archive) => return Self::for_single_archive(archive, config),
                Err(exn) => exn.recover(),
            };

            let msg = format!("Neither an archive nor a directory: \"{}\"", path.display());
            let exn = Exn::raise_all(ErrorMessage::new(msg), [dir_exn, archive_exn]);
            Err(exn)
        };

        let err = || ErrorMessage::new("Failed to collect all archives");

        stdout("Looking for images to convert in archives...");

        let jobs = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .flatten();
        let jobs = ArchiveJobs::aggregate(jobs).map(Self::Archives);
        Ok(jobs)
    }

    /// Create [`ConvertJob::Directories`].
    fn on_directories(
        paths: VecDeque<PathBuf>,
        config: &ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        let collect_single = |path| {
            let root = Directory::new(path)?.map_err(Exn::discard_recovery)?;
            Self::for_images_within_dir(root, config)
        };

        stdout("Looking for images to convert in directories...");

        let jobs = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .flatten();
        let jobs = RecursiveDirJobs::aggregate(jobs).map(Self::Directories);
        Ok(jobs)
    }

    /// Run this job.
    fn run(self, dry_run: bool) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("failed to run conversion job");

        self.dry_run().or_raise(err)?;
        if dry_run {
            return Ok(());
        }

        let collection_type = match self {
            Self::Archives(_) => JobsBarTitle::Archives,
            Self::Directories(_) => JobsBarTitle::Directories,
        };
        let bars = Bars::new(collection_type);

        match self {
            Self::Archives(jobs) => jobs.run(&bars)?,
            Self::Directories(jobs) => jobs.run(&bars)?,
        }

        bars.finish();
        Ok(())
    }

    /// Check if we can run this job, and print out statistics.
    fn dry_run(&self) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Issue encountered during dry run");

        self.check_tools().or_raise(err)?;
        self.print_statistics();

        let paths: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.jobs().map(Job::path),
            Self::Directories(jobs) => &mut jobs.jobs().map(Job::path),
        };

        for path in paths {
            info!("Got files to convert for \"{}\"", path.display());
        }

        Ok(())
    }

    /// Check if all tools needed for this job are actually available.
    fn check_tools(&self) -> Result<(), Exn<ErrorMessage>> {
        let iter: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.jobs().flat_map(Job::iter),
            Self::Directories(jobs) => &mut jobs.jobs().flat_map(Job::iter),
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
            bail!(ErrorMessage::new(msg))
        }
        Ok(())
    }

    /// Print out statistics on how many images would get converted by this job.
    fn print_statistics(&self) {
        let collections = match self {
            Self::Archives(jobs) => jobs.jobs().count(),
            Self::Directories(jobs) => jobs.jobs().count(),
        };

        let images = match self {
            Self::Archives(jobs) => &mut jobs.jobs().flat_map(Job::iter).count(),
            Self::Directories(jobs) => &mut jobs.jobs().flat_map(Job::iter).count(),
        };

        let coll_type = match self {
            Self::Archives(_) => "archives",
            Self::Directories(_) => "directories",
        };

        stdout(format!(
            "Found {collections} {coll_type}, with a total of {images} images to convert"
        ));
    }

    /// Create an [`ArchiveJobs`] for a single archive.
    fn for_single_archive(
        archive: ArchivePath,
        config: &ConversionConfig,
    ) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to create conversion job on a single archive");

        info!("Checking archive \"{}\"", archive.display());
        ArchiveJobs::single(archive, config).or_raise(err)
    }

    /// Create an [`ArchiveJobs`] for all archives in a directory.
    fn for_archives_in_dir(
        root: &Directory,
        config: &ConversionConfig,
    ) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
        let err =
            || ErrorMessage::new("Failed to create conversion job for all archives in a directory");

        info!("Checking archives directory \"{}\"", root.display());
        ArchiveJobs::collect(root, config).or_raise(err)
    }

    /// Create a [`RecursiveDirJobs`] for a directory.
    fn for_images_within_dir(
        root: Directory,
        config: &ConversionConfig,
    ) -> Result<Option<RecursiveDirJobs>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to create conversion job for a directory");

        info!("Checking root directory recursively \"{}\"", root.display());
        RecursiveDirJobs::single(root, config).or_raise(err)
    }
}

/// Specifies the kind of main job to create, with corresponding configuration
pub enum MainJobConfig {
    /// Run a statistics job,
    Stats,
    /// Run a conversion job.
    Convert(ConversionConfig),
}

impl MainJobConfig {
    /// Setup the configuration for the main job from user provided arguments.
    pub fn new(args: &Args) -> Self {
        const ONE: NonZeroUsize = NonZeroUsize::new(1).unwrap();

        let n_workers = match args.workers {
            Some(Some(value)) => value,
            Some(None) => ONE,
            None => thread::available_parallelism().unwrap_or(ONE),
        };

        args.command.target().map_or(Self::Stats, |target| {
            Self::Convert(ConversionConfig {
                target,
                n_workers,
                forced: args.force,
            })
        })
    }
}
