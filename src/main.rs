//! This programm is used to convert image files within Zip archives from one format to another.

mod convert;
mod error;
mod spawn;

use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::thread;

use clap::{self, Parser as _};
use exn::{ErrorExt as _, Exn, OptionExt as _, ResultExt as _, bail};
use tracing::{debug, error, info};

use crate::convert::archive::ArchivePath;
use crate::convert::collections::{ArchiveJobs, RecursiveDirJobs};
use crate::convert::dir::Directory;
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirImages, ImageInfo};
use crate::convert::{Configuration, Job, JobCollection as _};
use crate::error::{ErrorMessage, got_interrupted};

/// The program entry point.
///
/// It's only purpose is to log all errors bubbling up until here.
fn main() -> Result<(), Exn<ErrorMessage>> {
    let ret = real_main();

    match ret {
        Ok(()) => Ok(()),
        Err(exn) if got_interrupted(&exn) => {
            stderr("Got interrupted");
            Ok(())
        }
        Err(exn) => Err(exn),
    }
}

/// The application's entry point.
fn real_main() -> Result<(), Exn<ErrorMessage>> {
    const ONE: NonZeroUsize = NonZeroUsize::new(1).unwrap();

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

    let n_workers = match matches.workers {
        Some(Some(value)) => value,
        Some(None) => ONE,
        None => thread::available_parallelism().unwrap_or(ONE),
    };

    let paths = VecDeque::from(matches.paths);

    let config = matches.command.target().map(|target| Configuration {
        target,
        n_workers,
        forced: matches.force,
    });

    let main_job = match matches.no_archive {
        true => MainJob::on_directories(paths, config.as_ref()).or_raise(err)?,
        false => MainJob::on_archive(paths, config.as_ref()).or_raise(err)?,
    };

    let Some(main_job) = main_job else {
        stdout("Nothing to do");
        return Ok(());
    };

    main_job.run(matches.dry_run).or_raise(err)?;
    Ok(())
}

/// Convert images within comic archives to newer image formats.
///
/// Convert images within Comic Book Zip (CBZ) archives, although it also works with normal zip
/// files. By default only converts Jpeg and Png to the target format or decode any formats to
/// Png and Jpeg. The new archive with converted images is placed adjacent to the original, so this
/// operation is non-destructive.
#[derive(clap::Parser)]
#[command(version)]
struct Args {
    /// All images within the archive(s) are converted to this format
    #[arg(required = true)]
    command: Command,

    /// Path to cbz files or directories containing cbz files
    ///
    /// When providing directories, only top-level archives are considered for conversion.
    #[arg(default_value = ".", num_args = 1..)]
    paths: Vec<PathBuf>,

    /// Number of processes spawned
    ///
    /// Uses as many processes as you have cores by default. When used as a flag only spawns a
    /// single process at a time.
    #[expect(clippy::option_option)]
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

/// The sub command to run on found archives or directories.
#[derive(clap::ValueEnum, Clone, Copy)]
enum Command {
    /// Collect statistics on the images found.
    Stats,
    /// Convert to Jpeg.
    Jpeg,
    /// Convert to PNG.
    Png,
    /// Convert to AVIF.
    Avif,
    /// Convert to JXL.
    Jxl,
    /// Convert to WebP.
    Webp,
}

impl Command {
    /// Get the target format, if command is to convert.
    const fn target(self) -> Option<ImageFormat> {
        use ImageFormat::*;

        match self {
            Self::Stats => None,
            Self::Jpeg => Some(Jpeg),
            Self::Png => Some(Png),
            Self::Avif => Some(Avif),
            Self::Jxl => Some(Jxl),
            Self::Webp => Some(Webp),
        }
    }
}

/// The top-level task of the application, as determined by user arguments.
enum MainJob {
    /// We print statistics.
    Stats(StatsJob),
    /// We convert images.
    Convert(ConvertJob),
}

impl MainJob {
    /// Create [`MainJob::Archives`], combining all found archives into a single job collection.
    fn on_archive(
        paths: VecDeque<PathBuf>,
        config: Option<&Configuration>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let job = match config {
            Some(config) => ConvertJob::on_archive(paths, config)?.map(Self::Convert),
            None => StatsJob::on_archive(paths).map(Some)?.map(Self::Stats),
        };
        Ok(job)
    }

    /// Create [`MainJob::Directories`], combining all directories into a single job collection.
    fn on_directories(
        paths: VecDeque<PathBuf>,
        config: Option<&Configuration>,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let job = match config {
            Some(config) => ConvertJob::on_directories(paths, config)?.map(Self::Convert),
            None => StatsJob::on_directories(paths).map(Some)?.map(Self::Stats),
        };
        Ok(job)
    }

    /// Run this job.
    fn run(self, dry_run: bool) -> Result<(), Exn<ErrorMessage>> {
        match self {
            Self::Stats(job) => job.run(),
            Self::Convert(job) => job.run(dry_run)?,
        }
        Ok(())
    }
}

/// Our job is to print statistics about the images we find.
enum StatsJob {
    /// We work on archives.
    Archives(Vec<ArchiveImages>),
    /// We work on directories.
    Directories(Vec<DirImages>),
}

impl StatsJob {
    /// Create a [`StatsJob::Archives`].
    fn on_archive(paths: VecDeque<PathBuf>) -> Result<Self, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path)? {
                Ok(root) => return stats_for_archives_in_dir(&root),
                Err(exn) => exn.recover(),
            };

            let (path, archive_exn) = match ArchivePath::new(path) {
                Ok(archive) => return stats_for_single_archive(archive).map(|a| vec![a]),
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
            DirImages::new(root)
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
        counts.sort_by_key(|(f, _)| f.ext());
        for (format, count) in &counts {
            let format = format.ext();
            stdout(format!("{format}: {count}"));
        }
        let total: usize = counts.iter().map(|(_, c)| c).sum();
        stdout("---");
        stdout(format!("total: {total}"));
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
    /// Create a [`StatsJob::Archives`].
    fn on_archive(
        paths: VecDeque<PathBuf>,
        config: &Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path)? {
                Ok(root) => return convert_archives_in_dir(&root, config),
                Err(exn) => exn.recover(),
            };

            let (path, archive_exn) = match ArchivePath::new(path) {
                Ok(archive) => return convert_single_archive(archive, config),
                Err(exn) => exn.recover(),
            };

            let path = path.display();
            let msg = format!("Neither an archive nor a directory: \"{path}\"");
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
        config: &Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        let collect_single = |path| {
            let root = Directory::new(path)?.map_err(Exn::discard_recovery)?;
            convert_images_in_dir_recursively(root, config)
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
            Self::Archives(_) => convert::JobsBarTitle::Archives,
            Self::Directories(_) => convert::JobsBarTitle::Directories,
        };
        let bars = convert::Bars::new(collection_type);

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
}

/// Create a [`convert::ArchiveJobs`] for a single archive.
fn stats_for_single_archive(archive: ArchivePath) -> Result<ArchiveImages, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to search images in a single archive");

    info!("Checking archive {archive:?}");
    ArchiveImages::new(archive).or_raise(err)
}

/// Create a [`convert::ArchiveJobs`] for all archives in a directory.
fn stats_for_archives_in_dir(root: &Directory) -> Result<Vec<ArchiveImages>, Exn<ErrorMessage>> {
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

/// Create a [`convert::ArchiveJobs`] for a single archive.
fn convert_single_archive(
    archive: ArchivePath,
    config: &Configuration,
) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to create conversion job on a single archive");

    info!("Checking archive {archive:?}");
    ArchiveJobs::single(archive, config).or_raise(err)
}

/// Create a [`convert::ArchiveJobs`] for all archives in a directory.
fn convert_archives_in_dir(
    root: &Directory,
    config: &Configuration,
) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
    let err =
        || ErrorMessage::new("Failed to create conversion job for all archives in a directory");

    info!("Checking archives directory \"{}\"", root.display());
    ArchiveJobs::collect(root, config).or_raise(err)
}

/// Create a [`convert::RecursiveDirJobs`] for a directory.
fn convert_images_in_dir_recursively(
    root: Directory,
    config: &Configuration,
) -> Result<Option<RecursiveDirJobs>, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to create conversion job for a directory");

    info!("Checking root directory recursively {root:?}");
    RecursiveDirJobs::single(root, config).or_raise(err)
}

/// Initialize the logger as requested.
fn init_logger(path: &Path, level: tracing::Level) -> Result<(), Exn<ErrorMessage>> {
    let err = || {
        let path = path.display();
        ErrorMessage::new(format!("Failed to initialize logging to file \"{path}\""))
    };

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
        let directory = directory.display();
        let msg = format!("Directory does not exist: \"{directory}\"");
        return Err(err(msg));
    }
    if path.exists() && !path.is_file() {
        let path = path.display();
        let msg = format!("The path to the log file is not a regular file: \"{path}\"");
        return Err(err(msg));
    }
    let Some(file_name) = path.file_name() else {
        let path = path.display();
        let msg = format!("The filename is empty: \"{path}\"");
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

/// Print a message to stdout (and logs)
fn stdout(msg: impl AsRef<str>) {
    let msg = msg.as_ref();
    println!("{msg}");
    info!("{msg}");
}

/// Print a message to stderr (and logs)
fn stderr(msg: impl AsRef<str>) {
    let msg = msg.as_ref();
    eprintln!("{msg}");
    error!("{msg}");
}
