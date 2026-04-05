//! The library for the `cbz_in` executable.
mod command;
mod convert;
mod error;
mod spawn;
mod stats;

use std::collections::HashSet;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use exn::{ErrorExt as _, Exn, OptionExt as _, ResultExt as _};
use tracing::{debug, info};

use crate::command::FoundImages;
use crate::convert::FilesystemRoot;
pub use crate::convert::archive::ArchivePath;
pub use crate::convert::dir::Directory;
pub use crate::convert::image::{ImageFormat, jxl_is_compressed_jpeg};
pub use crate::convert::search::{ArchiveImages, DirectoryImages, ImageInfo, Images};
pub use crate::error::{CompactReport, ErrorMessage, got_interrupted};
pub use crate::spawn::{ManagedChild, list_archive_files};

/// The application's entry point.
pub fn entry_point(args: Args) -> Result<(), Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Error when executing program");

    if !args.no_log {
        init_logger(&args.log_path, args.level).or_raise(err)?;
    }

    let cmd = std::env::args_os()
        .map(|s| s.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    info!("starting new execution as {cmd:?}");
    let cwd = std::env::current_dir().unwrap_or_default();
    info!("working directory: {:?}", cwd);

    let paths = args.paths.into_iter();
    let root = match args.no_archive {
        true => FilesystemRoot::Directory,
        false => FilesystemRoot::Archive,
    };
    let Some(found) = FoundImages::search(paths, root)? else {
        stdout("Nothing to do");
        return Ok(());
    };

    let source = ConversionSource::to_filter_set(&args.from);
    debug!("sources: {source:?}");
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

/// Convert images within comic archives to newer image formats.
///
/// Convert images within Comic Book Zip (CBZ) archives, although it also works with normal zip
/// files. By default only converts Jpeg and Png to the target format or decode any formats to
/// Png and Jpeg. The new archive with converted images is placed adjacent to the original, so this
/// operation is non-destructive.
#[expect(clippy::struct_excessive_bools)]
#[derive(clap::Parser)]
#[command(version)]
pub struct Args {
    /// All images within the archive(s) are converted to this format
    #[command(subcommand)]
    pub command: Command,

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
    pub from: Vec<ConversionSource>,

    /// Path to cbz files or directories containing cbz files
    ///
    /// When providing directories, only top-level archives are considered for conversion.
    #[arg(default_value = ".", num_args = 1.., global = true)]
    pub paths: Vec<PathBuf>,

    /// Number of processes spawned
    ///
    /// Uses as many processes as you have cores by default. When used as a flag only spawns a
    /// single process at a time.
    #[arg(short = 'j', long, global = true)]
    pub workers: Option<Option<NonZeroUsize>>,

    /// Check if all tools are available to perform conversions.
    #[arg(long, global = true)]
    pub dry_run: bool,

    /// Check if all tools are available to perform conversions.
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Convert images in the directory directly (recursively)
    ///
    /// This will create a copy of your directory structure using hard links. This means your data
    /// is not copied as both structures point to the same underlying files. The only difference
    /// between both directory structures are the converted images found by a recursive search.
    ///
    /// Note that this does not traverse mount points.
    #[arg(long, global = true)]
    pub no_archive: bool,

    /// The path to the log file that gets written
    #[arg(
        long,
        value_name = "LOG_FILE",
        default_value = "/tmp/cbz_in.log",
        global = true
    )]
    pub log_path: PathBuf,

    /// Detail level of logging
    #[arg(long, default_value = "info", global = true)]
    pub level: tracing::Level,

    /// Disable any logging
    ///
    /// Useful for tests
    #[arg(long, global = true)]
    pub no_log: bool,
}

/// The sub command to run on found archives or directories.
#[derive(clap::Subcommand, Clone, Copy)]
pub enum Command {
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

impl Display for ConversionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::All => "all",
            Self::Jpeg => "jpeg",
            Self::Png => "png",
            Self::Avif => "avif",
            Self::Jxl => "jxl",
            Self::Webp => "webp",
        };
        f.write_str(str)
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
        /// Does not change conversion behavior for any other file format besides jpeg.
        #[arg(long)]
        lossy: bool,
    },
    /// Convert to WebP.
    Webp,
}

impl ConversionTarget {
    /// Get the target type, discarding conversion metadata.
    #[must_use]
    pub const fn format(self) -> ImageFormat {
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
