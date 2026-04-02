//! This programm is used to convert image files within Zip archives from one format to another.

mod command;
mod convert;
mod error;
mod spawn;
mod stats;

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::ops::Not as _;
use std::path::{Path, PathBuf};

use clap::{self, Parser as _};
use exn::{ErrorExt as _, Exn, OptionExt as _, ResultExt as _};
use tracing::{debug, error, info};

use crate::convert::archive::{ArchiveJob, ArchivePath};
use crate::convert::dir::{Directory, RecursiveDirJob};
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirImages, ImageCollection};
use crate::convert::{Bars, ConversionConfig, Job};
use crate::error::{CompactReport, ErrorMessage, got_interrupted};

/// The program entry point.
///
/// It's only purpose is to log all errors bubbling up until here.
fn main() {
    let ret = real_main();

    match ret {
        Ok(()) => (),
        Err(exn) if got_interrupted(&exn) => {
            stderr("Got interrupted");
        }
        Err(exn) => {
            let report = CompactReport::new(&exn);
            println!("{report}");
            error!("Application error:\n{report:?}");
        }
    }
}

/// The application's entry point.
fn real_main() -> Result<(), Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Error when executing program");

    let args = Args::parse();

    init_logger(&args.log_path, args.level).or_raise(err)?;

    let cmd = std::env::args_os()
        .map(|s| s.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    info!("starting new execution as {cmd:?}");
    let cwd = std::env::current_dir().unwrap_or_default();
    info!("working directory: {:?}", cwd);

    let source = ConversionSource::to_filter_set(&args.from);

    let paths = args.paths.into_iter();
    let found = match args.no_archive {
        true => FoundCollections::on_dirs(paths)?,
        false => FoundCollections::on_archives(paths)?,
    };

    let Some(filtered) = found.filter(&source) else {
        stdout("Nothing to do");
        return Ok(());
    };

    match args.command {
        Command::Stats => {
            filtered.print_stats(args.verbose);
        }
        Command::Convert(target) => {
            const ONE: NonZeroUsize = NonZeroUsize::new(1).unwrap();
            let workers = match args.workers {
                Some(Some(value)) => value,
                Some(None) => ONE,
                None => std::thread::available_parallelism().unwrap_or(ONE),
            };
            filtered.convert(target, workers)?;
        }
    }

    Ok(())
}

/// A path that points either to a directory or to an archive.
enum DirOrArchive {
    /// The path points to a directory
    Directory(Directory),
    /// The path points to an archive
    Archive(ArchivePath),
}

impl DirOrArchive {
    /// Verify the path points either to a directory or an archive.
    fn check(path: PathBuf) -> Result<Self, Exn<ErrorMessage>> {
        let (path, dir_exn) = match Directory::new(path)? {
            Ok(root) => return Ok(Self::Directory(root)),
            Err(path) => (path, ErrorMessage::new("Not a directory").raise()),
        };

        let (path, archive_exn) = match ArchivePath::new(path) {
            Ok(archive) => return Ok(Self::Archive(archive)),
            Err(exn) => exn.recover(),
        };

        let path = path.display();
        let msg = format!("Neither an archive nor a directory: \"{path}\"");
        let exn = Exn::raise_all(ErrorMessage::new(msg), [dir_exn, archive_exn]);
        Err(exn)
    }

    /// Convert this to a iterator over all applicable archives.
    ///
    /// When this is an archive directly, gives back just that one archive. When it is a directory,
    /// looks for any direct child files that are archives, and iterates over those.
    fn archive_iter(self) -> Result<impl Iterator<Item = ArchivePath>, Exn<ErrorMessage>> {
        match self {
            Self::Directory(dir) => {
                let flattened = Self::flatten_dir(&dir, true)?;
                Ok(either::Left(flattened.into_iter()))
            }
            Self::Archive(file) => Ok(either::Right(std::iter::once(file))),
        }
    }

    /// Find all child entries of this directory and collect those those that are archives.
    fn flatten_dir(dir: &Directory, verbose: bool) -> Result<Vec<ArchivePath>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("finding archives in directory");

        dir.read_dir()
            .or_raise(err)?
            .map(|dir_entry| {
                let path = dir_entry.or_raise(err)?.path();
                match ArchivePath::new(path) {
                    Ok(archive) => Ok(Some(archive)),
                    Err(exn) => {
                        let (path, exn) = exn.recover();
                        let path = path.display();
                        let report = CompactReport::new(&exn);
                        crate::verbose(verbose, format!("Skipping \"{path}\": {report}"));
                        Ok(None)
                    }
                }
            })
            .filter_map(Result::transpose)
            .collect()
    }
}

/// All collections found in the locations specified by the user.
enum FoundCollections {
    /// We looked for images in archives.
    Archives(Vec<ArchiveImages>),
    /// We looked for images in directories.
    Directories(Vec<DirImages>),
}

impl FoundCollections {
    /// Look for images in archives.
    fn on_archives(paths: impl Iterator<Item = PathBuf>) -> Result<Self, Exn<ErrorMessage>> {
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
        Ok(Self::Archives(found))
    }

    /// Look for images in directories.
    fn on_dirs(paths: impl Iterator<Item = PathBuf>) -> Result<Self, Exn<ErrorMessage>> {
        let found = paths
            .into_iter()
            .map(|path| -> Result<Option<DirImages>, Exn<ErrorMessage>> {
                let dir = Directory::new(path)?.map_err(|path| {
                    let path = path.display();
                    let msg = format!("Path is not a directory: \"{path}\"");
                    ErrorMessage::new(msg).raise()
                })?;

                let images = DirImages::search_recursive(dir)?
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

        Ok(Self::Directories(found))
    }

    /// Filter out all images such that only those remain that are specified in the filter.
    fn filter(self, filter: &HashSet<ImageFormat>) -> Option<Self> {
        fn filter_item<T>(item: T, filter: &HashSet<ImageFormat>) -> Option<T>
        where
            T: ImageCollection,
        {
            item.filter(filter)
                .inspect_err(|path| {
                    debug!(
                        "Archive has no images left after filtering: \"{}\"",
                        path.display()
                    );
                })
                .ok()
        }

        match self {
            Self::Archives(items) => {
                let vec = items
                    .into_iter()
                    .filter_map(|item| filter_item(item, filter))
                    .collect::<Vec<_>>();
                Some(vec).filter(|v| !v.is_empty()).map(Self::Archives)
            }
            Self::Directories(items) => {
                let vec = items
                    .into_iter()
                    .filter_map(|item| filter_item(item, filter))
                    .collect::<Vec<_>>();
                Some(vec).filter(|v| !v.is_empty()).map(Self::Directories)
            }
        }
    }

    /// Print the number of images found per image format to stdout.
    fn print_stats(&self, verbose: bool) {
        let mut all_stats = stats::Stats::new();

        match self {
            Self::Archives(items) => {
                let count = items.len();
                stdout(format!("Searched {count} archives:"));

                for item in items {
                    let stats = stats::Stats::compute(item.infos());
                    all_stats.combine(&stats);

                    if verbose {
                        let header = format!("\"{}\":", item.path().display());
                        stdout(header);
                        stats.print_per_format();
                        stdout(""); // new line
                    }
                }
            }
            Self::Directories(items) => {
                let count = items.len();
                stdout(format!("Searched {count} directories:"));

                for item in items {
                    let stats = stats::Stats::compute(item.infos());
                    all_stats.combine(&stats);

                    if verbose {
                        let header = format!("\"{}\":", item.path().display());
                        stdout(header);
                        print_stats_per_format(&stats);
                        stdout(""); // new line
                    }
                }
            }
        }

        stdout("");
        print_stats_per_format(&all_stats);
        stdout("---");
        print_stats_total(&all_stats);
    }

    /// Run conversion on the found images.
    fn convert(
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
pub fn print_stats_per_format(stats: &stats::Stats) {
    let mut counts = Vec::from_iter(stats.inner.clone());
    counts.sort_unstable_by_key(|(f, _)| f.ext());
    for (format, count) in &counts {
        let format = format.ext();
        stdout(format!("{format}: {count}"));
    }
}

/// Print out the total number of images found.
pub fn print_stats_total(stats: &stats::Stats) {
    let total: usize = stats.inner.values().sum();
    stdout(format!("total: {total}"));
}

/// Helper object to run conversion
enum RunConversion {
    /// We run conversion on archives.
    Archives(Vec<ArchiveJob>),
    /// We run conversion on directories.
    Directories(Vec<RecursiveDirJob>),
}

impl RunConversion {
    /// Prepare conversion jobs.
    fn new(
        found: FoundCollections,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let out = match found {
            FoundCollections::Archives(items) => {
                let items = items
                    .into_iter()
                    .map(|item| {
                        let job = ArchiveJob::new(item, config)?
                            .inspect_err(|error::NothingToDo { path, reason }| {
                                debug!("{reason}: Skip \"{}\"", path.display());
                            })
                            .ok();
                        Ok(job)
                    })
                    .filter_map(Result::transpose)
                    .collect::<Result<Vec<_>, Exn<ErrorMessage>>>()?;

                items.is_empty().not().then_some(Self::Archives(items))
            }
            FoundCollections::Directories(items) => {
                let items = items
                    .into_iter()
                    .map(|item| {
                        let job = RecursiveDirJob::new(item, config)?
                            .inspect_err(|error::NothingToDo { path, reason }| {
                                debug!("{reason}: Skip \"{}\"", path.display());
                            })
                            .ok();
                        Ok(job)
                    })
                    .filter_map(Result::transpose)
                    .collect::<Result<Vec<_>, Exn<ErrorMessage>>>()?;

                items.is_empty().not().then_some(Self::Directories(items))
            }
        };

        Ok(out)
    }

    /// Run the conversion for real.
    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        match self {
            Self::Archives(items) => {
                let bars = Bars::new(convert::JobsBarTitle::Archives);
                bars.jobs.set_length(items.len() as u64);

                for item in items {
                    bars.println(format!("Converting \"{}\"", item.path().display()));
                    item.run(&bars.images)?;
                    bars.jobs.inc(1);
                }

                bars.finish();
            }
            Self::Directories(items) => {
                let bars = Bars::new(convert::JobsBarTitle::Directories);
                bars.jobs.set_length(items.len() as u64);

                for item in items {
                    bars.println(format!("Converting \"{}\"", item.path().display()));
                    item.run(&bars.images)?;
                    bars.jobs.inc(1);
                }

                bars.finish();
            }
        }
        Ok(())
    }

    /// Check if we can run this job, and print out statistics.
    fn test(&self) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Doing dry run");

        self.check_tools().or_raise(err)?;
        self.print_statistics();

        let paths: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.iter().map(Job::path),
            Self::Directories(jobs) => &mut jobs.iter().map(Job::path),
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
    #[command(subcommand)]
    command: Command,

    /// Only convert images of this format.
    ///
    /// Delimit multiple formats with a comma (,), or specify "all". By default uses "jpeg,png".
    #[arg(
        short = 's',
        long,
        num_args = 1..,
        value_delimiter = ',',
        default_value = "jpeg,png",
        global = true
    )]
    from: Vec<ConversionSource>,

    /// Path to cbz files or directories containing cbz files
    ///
    /// When providing directories, only top-level archives are considered for conversion.
    #[arg(default_value = ".", num_args = 1.., global = true)]
    paths: Vec<PathBuf>,

    /// Number of processes spawned
    ///
    /// Uses as many processes as you have cores by default. When used as a flag only spawns a
    /// single process at a time.
    #[expect(clippy::option_option)]
    #[arg(short = 'j', long, global = true)]
    workers: Option<Option<NonZeroUsize>>,

    /// Check if all tools are available to perform conversions.
    #[arg(long, global = true)]
    dry_run: bool,

    /// Check if all tools are available to perform conversions.
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Convert images in the directory directly (recursively)
    ///
    /// This will create a copy of your directory structure using hard links. This means your data
    /// is not copied as both structures point to the same underlying files. The only difference
    /// between both directory structures are the converted images found by a recursive search.
    ///
    /// Note that this does not traverse mount points.
    #[arg(long, global = true)]
    no_archive: bool,

    /// The path to the log file that gets written
    #[arg(
        long,
        value_name = "LOG_FILE",
        default_value = "/tmp/cbz_in.log",
        global = true
    )]
    log_path: PathBuf,

    /// Detail level of logging
    #[arg(long, default_value = "info", global = true)]
    level: tracing::Level,
}

/// The sub command to run on found archives or directories.
#[derive(clap::Subcommand, Clone, Copy)]
enum Command {
    /// Collect statistics on the images found.
    Stats,
    /// Convert found images to another image format.
    #[command(flatten)]
    Convert(ConversionTarget),
}

/// The target image format to convert all images to.
///
/// This is basically a copy of of [`ImageFormat`], just with the helper variant
/// [`ConversionSource::All`].
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConversionSource {
    /// Convert from all formats.
    All,
    /// Convert from JPEG.
    Jpeg,
    /// Convert from PNG.
    Png,
    /// Convert from AVIF.
    Avif,
    /// Convert from JXL.
    Jxl,
    /// Convert from WebP.
    Webp,
}

impl ConversionSource {
    /// Create the vector
    fn to_filter_set(sources: &[Self]) -> HashSet<ImageFormat> {
        assert!(!sources.is_empty(), "never expect empty sources");

        sources
            .iter()
            .flat_map(|&s| match s.try_into() {
                Ok(s) => either::Left(std::iter::once(s)),
                Err(()) => either::Right(ImageFormat::ALL.iter().copied()),
            })
            .collect()
    }
}

impl TryFrom<ConversionSource> for ImageFormat {
    type Error = ();

    fn try_from(value: ConversionSource) -> Result<Self, Self::Error> {
        use ConversionSource as S;

        match value {
            S::All => Err(()),
            S::Jpeg => Ok(Self::Jpeg),
            S::Png => Ok(Self::Png),
            S::Avif => Ok(Self::Avif),
            S::Jxl => Ok(Self::Jxl),
            S::Webp => Ok(Self::Webp),
        }
    }
}

/// The target image format to convert all images to.
#[derive(clap::Subcommand, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConversionTarget {
    /// Convert to JPEG.
    Jpeg,
    /// Convert to PNG.
    Png,
    /// Convert to AVIF.
    Avif,
    /// Convert to JXL.
    Jxl {
        /// Jpegs are reencoded, not compressed.
        ///
        /// Does not change conversion behavior for any other file format besides than jpeg.
        #[arg(long)]
        lossy: bool,
    },
    /// Convert to WebP.
    Webp,
}

impl ConversionTarget {
    /// Get the target type, discarding conversion metadata.
    const fn format(self) -> ImageFormat {
        use ImageFormat as I;

        match self {
            Self::Jpeg => I::Jpeg,
            Self::Png => I::Png,
            Self::Avif => I::Avif,
            Self::Jxl { .. } => I::Jxl,
            Self::Webp => I::Webp,
        }
    }
}

/// Initialize the logger as requested.
fn init_logger(path: &Path, level: tracing::Level) -> Result<(), Exn<ErrorMessage>> {
    let err = || {
        let path = path.display();
        ErrorMessage::new(format!("Initializing logging to file \"{path}\""))
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

/// Conditionally print a message to stdout (and logs)
fn verbose(show_on_terminal: bool, msg: impl AsRef<str>) {
    let msg = msg.as_ref();
    if show_on_terminal {
        println!("{msg}");
    }
    info!("{msg}");
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
