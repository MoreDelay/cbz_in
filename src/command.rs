//! Contains the main, high-level job which performs the command chosen by the user

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::thread;

use exn::Exn;

use crate::Args;
use crate::convert::{ConversionConfig, ConvertJob};
use crate::error::ErrorMessage;
use crate::stats::{StatsConfig, StatsJob};

/// The top-level task of the application, as determined by user arguments.
pub struct MainJob(MainJobImpl);

impl MainJob {
    /// Execute the main job on archives.
    ///
    /// Convert all found images according to `config`. If `config` is `None`, then we only collect
    /// statistics.
    pub fn on_archives(
        paths: VecDeque<PathBuf>,
        config: MainJobConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        MainJobImpl::on_archives(paths, config).map(|opt| opt.map(Self))
    }

    /// Execute the main job on directories.
    ///
    /// Convert all found images according to `config`. If `config` is `None`, then we only collect
    /// statistics.
    pub fn on_directories(
        paths: VecDeque<PathBuf>,
        config: MainJobConfig,
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
        config: MainJobConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        use MainJobConfig::*;

        let job = match config {
            Stats(config) => StatsJob::on_archives(paths, config)?.map(Self::Stats),
            Convert(config) => ConvertJob::on_archives(paths, config)?.map(Self::Convert),
        };
        Ok(job)
    }

    /// Create the main job on directories.
    fn on_directories(
        paths: VecDeque<PathBuf>,
        config: MainJobConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        use MainJobConfig::*;

        let job = match config {
            Stats(config) => StatsJob::on_directories(paths, config)?.map(Self::Stats),
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

/// Specifies the kind of main job to create, with corresponding configuration
pub enum MainJobConfig {
    /// Run a statistics job,
    Stats(StatsConfig),
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

        match args.command {
            crate::Command::Stats { filter } => Self::Stats(StatsConfig {
                filter,
                verbose: args.verbose,
            }),
            crate::Command::Convert(target) => Self::Convert(ConversionConfig {
                target: target.into(),
                n_workers,
                forced: args.force,
            }),
        }
    }
}
