//! This programm is used to convert image files within Zip archives from one format to another.

mod convert;
mod error;
mod spawn;

use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::thread;

use clap::Parser;
use exn::{ErrorExt, Exn, OptionExt, ResultExt, bail};
use tracing::info;

use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
use crate::convert::image::ImageFormat;
use crate::convert::{ArchiveJobs, Configuration, Job, JobCollection, RecursiveDirJobs};
use crate::error::{ErrorMessage, got_interrupted};

/// The program entry point.
///
/// It's only purpose is to log all errors bubbling up until here.
fn main() -> Result<(), Exn<ErrorMessage>> {
    let ret = real_main();

    match ret {
        Ok(()) => Ok(()),
        Err(exn) if got_interrupted(&exn) => {
            eprintln!("Got interrupted");
            Ok(())
        }
        Err(exn) => Err(exn),
    }
}

/// The application's entry point.
fn real_main() -> Result<(), Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to run conversion jobs");

    let matches = Args::parse();

    if let Some(log_path) = matches.log {
        init_logger(&log_path, matches.level).or_raise(err)?;
    }

    let cmd = std::env::args_os()
        .map(|s| s.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    info!("starting new execution as {cmd:?}");
    let cwd = std::env::current_dir().unwrap_or_default();
    info!("working directory: {:?}", cwd);

    const ONE: NonZeroUsize = NonZeroUsize::new(1).unwrap();
    let n_workers = match matches.workers {
        Some(Some(value)) => value,
        Some(None) => ONE,
        None => thread::available_parallelism().unwrap_or(ONE),
    };

    let config = Configuration {
        target: matches.format,
        n_workers,
        forced: matches.force,
    };

    let paths = VecDeque::from(matches.paths);

    let main_job = match matches.no_archive {
        true => MainJob::collect_directory_jobs(paths, &config).or_raise(err)?,
        false => MainJob::collect_archive_jobs(paths, &config).or_raise(err)?,
    };
    let Some(main_job) = main_job else {
        eprintln!("Nothing to do");
        return Ok(());
    };

    main_job.dry_run().or_raise(err)?;
    if !matches.dry_run {
        main_job.run().or_raise(err)?;
    }
    Ok(())
}

/// Convert images within comic archives to newer image formats.
///
/// Convert images within Comic Book Zip (CBZ) archives, although it also works with normal zip
/// files. By default only converts Jpeg and Png to the target format or decode any formats to
/// Png and Jpeg. The new archive with converted images is placed adjacent to the original, so this
/// operation is non-destructive.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// All images within the archive(s) are converted to this format
    #[arg(required = true)]
    format: ImageFormat,

    /// Path to cbz files or directories containing cbz files
    ///
    /// When providing directories, only top-level archives are considered for conversion.
    #[arg(default_value = ".", num_args = 1..)]
    paths: Vec<PathBuf>,

    /// Number of processes spawned
    ///
    /// Uses as many processes as you have cores by default. When used as a flag only spawns a
    /// single process at a time.
    #[arg(short = 'j', long)]
    workers: Option<Option<NonZeroUsize>>,

    /// Convert all images of all formats.
    #[arg(short, long)]
    force: bool,

    /// Check if all tools are available to perform conversions.
    #[arg(short, long)]
    dry_run: bool,

    /// Convert images in the directory directly (recursively)
    ///
    /// This will create a copy of your directory structure using hard links. This means your data
    /// is not copied as both structures point to the same underlying files. The only difference
    /// between both directory structures are the converted images found in a recursive search.
    #[arg(long)]
    no_archive: bool,

    /// Write a log file
    #[arg(
        long ,
        value_name = "LOG_FILE",
        num_args(0..=1),
        default_missing_value = "./cbz_in.log",
    )]
    log: Option<PathBuf>,

    /// Detail level of logging
    #[arg(long, default_value = "info")]
    level: tracing::Level,
}

/// The top-level task of the application, as determined by user arguments.
enum MainJob {
    /// We work on archives.
    Archives(ArchiveJobs),
    /// We work on directories.
    Directories(RecursiveDirJobs),
}

impl MainJob {
    /// Create [MainJob::Archives], combining all found archives into a single job collection.
    fn collect_archive_jobs(
        paths: VecDeque<PathBuf>,
        config: &Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path) {
                Ok(root) => return archives_in_dir(root, config),
                Err(exn) => exn.recover(),
            };

            let (path, archive_exn) = match ArchivePath::new(path) {
                Ok(archive) => return single_archive(archive, config),
                Err(exn) => exn.recover(),
            };

            let msg = format!("Neither an archive nor a directory: {path:?}");
            let exn = Exn::raise_all(ErrorMessage::new(msg), [dir_exn, archive_exn]);
            Err(exn)
        };

        let err = || ErrorMessage::new("Failed to collect all archives");

        let jobs = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .flatten();
        let jobs = ArchiveJobs::new(jobs).map(Self::Archives);
        Ok(jobs)
    }

    /// Create [MainJob::Directories], combining all directories into a single job collection.
    fn collect_directory_jobs(
        paths: VecDeque<PathBuf>,
        config: &Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        let collect_single = |path| {
            let root = Directory::new(path).map_err(|e| e.discard_recovery())?;
            images_in_dir_recursively(root, config)
        };

        let jobs = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .flatten();
        let jobs = RecursiveDirJobs::new(jobs).map(Self::Directories);
        Ok(jobs)
    }

    /// Run this job.
    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        let collection_type = match self {
            MainJob::Archives(_) => convert::JobsBarTitle::Archives,
            MainJob::Directories(_) => convert::JobsBarTitle::Directories,
        };
        let bars = convert::Bars::new(collection_type);

        match self {
            MainJob::Archives(jobs) => jobs.run(&bars)?,
            MainJob::Directories(jobs) => jobs.run(&bars)?,
        }

        bars.finish();
        Ok(())
    }

    /// Check if we can run this job, and print out statistics.
    fn dry_run(&self) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Issue encountered during dry run");

        self.check_tools().or_raise(err)?;
        self.print_statistics();
        Ok(())
    }

    /// Check if all tools needed for this job are actually available.
    fn check_tools(&self) -> Result<(), Exn<ErrorMessage>> {
        let iter: &mut dyn Iterator<Item = _> = match self {
            MainJob::Archives(jobs) => &mut jobs.jobs().flat_map(|job| job.iter()),
            MainJob::Directories(jobs) => &mut jobs.jobs().flat_map(|job| job.iter()),
        };
        let required_tools = iter
            .flat_map(|job| job.plan().required_tools())
            .collect::<HashSet<_>>();
        let missing_tools = required_tools
            .into_iter()
            .flat_map(|tool| match tool.available() {
                Ok(true) => None,
                Ok(false) => Some(Ok(tool.name())),
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !missing_tools.is_empty() {
            let tools = missing_tools.join(", ");
            let msg = format!("Missing tools: {tools}");
            bail!(ErrorMessage::new(msg))
        }
        Ok(())
    }

    /// Print out statistics on how many images would get converted by this job.
    fn print_statistics(&self) {
        let collections = match self {
            MainJob::Archives(jobs) => jobs.jobs().count(),
            MainJob::Directories(jobs) => jobs.jobs().count(),
        };

        let images = match self {
            MainJob::Archives(jobs) => &mut jobs.jobs().flat_map(|job| job.iter()).count(),
            MainJob::Directories(jobs) => &mut jobs.jobs().flat_map(|job| job.iter()).count(),
        };

        let coll_type = match self {
            MainJob::Archives(_) => "archives",
            MainJob::Directories(_) => "directories",
        };

        println!("Found {collections} {coll_type}, with a total of {images} images to convert");
    }
}

/// Create a [convert::ArchiveJobs] for a single archive.
fn single_archive(
    archive: ArchivePath,
    config: &Configuration,
) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to create conversion job on a single archive");

    info!("Checking archive {archive:?}");
    ArchiveJobs::single(archive, config).or_raise(err)
}

/// Create a [convert::ArchiveJobs] for all archives in a directory.
fn archives_in_dir(
    root: Directory,
    config: &Configuration,
) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
    let err =
        || ErrorMessage::new("Failed to create conversion job for all archives in a directory");

    info!("Checking archives directory {root:?}");
    ArchiveJobs::collect(root, config).or_raise(err)
}

/// Create a [convert::RecursiveDirJobs] for a directory.
fn images_in_dir_recursively(
    root: Directory,
    config: &Configuration,
) -> Result<Option<RecursiveDirJobs>, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to create conversion job for a directory");

    info!("Checking root directory recursively {root:?}");
    RecursiveDirJobs::single(root, config).or_raise(err)
}

/// Initialize the logger as requested.
fn init_logger(path: &Path, level: tracing::Level) -> Result<(), Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to initialize logging to file {path:?}");

    let path = match path.is_absolute() {
        true => path,
        false => &PathBuf::from(".").join(path),
    };

    let directory = path.parent().ok_or_raise(err)?;

    // add another layer for error context
    let err = |msg| {
        let exn = ErrorMessage::new(msg).raise();
        exn.raise(err())
    };
    if !directory.is_dir() {
        let msg = format!("Directory does not exist: {directory:?}");
        return Err(err(msg));
    }
    if path.exists() && !path.is_file() {
        let msg = format!("The path to the log file is not a regular file: {path:?}");
        return Err(err(msg));
    }
    let Some(file_name) = path.file_name() else {
        let msg = format!("The filename is empty: {path:?}");
        return Err(err(msg));
    };

    let writer = tracing_appender::rolling::never(directory, file_name);

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(writer)
        .with_ansi(false)
        .init();

    Ok(())
}
