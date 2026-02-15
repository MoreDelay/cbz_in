//! This programm is used to convert image files within Zip archives from one format to another.

mod command;
mod convert;
mod error;
mod spawn;
mod stats;

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use clap::builder::ArgPredicate;
use clap::{self, Parser as _};
use exn::{ErrorExt as _, Exn, OptionExt as _, ResultExt as _};
use tracing::{error, info};

use crate::command::{MainJob, MainJobConfig};
use crate::convert::image::ImageFormat;
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
            let report = CompactReport::new(exn);
            println!("{report}");
            error!("Application error:\n{report:?}");
        }
    }
}

/// The application's entry point.
fn real_main() -> Result<(), Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Error during program execution");

    let args = Args::parse();

    if args.log {
        init_logger(&args.log_path, args.level).or_raise(err)?;
    }

    let cmd = std::env::args_os()
        .map(|s| s.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    info!("starting new execution as {cmd:?}");
    let cwd = std::env::current_dir().unwrap_or_default();
    info!("working directory: {:?}", cwd);

    let config = MainJobConfig::new(&args);
    let paths = VecDeque::from(args.paths);

    let main_job = match args.no_archive {
        true => MainJob::on_directories(paths, config).or_raise(err)?,
        false => MainJob::on_archives(paths, config).or_raise(err)?,
    };

    let Some(main_job) = main_job else {
        stdout("Nothing to do");
        return Ok(());
    };

    main_job.run(args.dry_run).or_raise(err)?;
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
struct Args {
    /// All images within the archive(s) are converted to this format
    #[command(subcommand)]
    command: Command,

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

    /// Convert all images of all formats.
    #[arg(short, long, global = true)]
    force: bool,

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

    /// Write a log file
    #[arg(
        long,
        default_value_if("log_path", ArgPredicate::IsPresent, "true"),
        default_value_if("level", ArgPredicate::IsPresent, "true"),
        global = true
    )]
    log: bool,

    /// The path to the log file that gets written
    #[arg(
        long,
        value_name = "LOG_FILE",
        default_value = "./cbz_in.log",
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
    Stats {
        /// Filter for a specific image format.
        #[arg(long, default_value = None)]
        filter: Option<ImageFormat>,
    },
    /// Convert found images to another image format.
    #[command(flatten)]
    Convert(ConversionTarget),
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
    Jxl,
    /// Convert to WebP.
    Webp,
}

impl From<ConversionTarget> for ImageFormat {
    fn from(value: ConversionTarget) -> Self {
        use ImageFormat::*;
        match value {
            ConversionTarget::Jpeg => Jpeg,
            ConversionTarget::Png => Png,
            ConversionTarget::Avif => Avif,
            ConversionTarget::Jxl => Jxl,
            ConversionTarget::Webp => Webp,
        }
    }
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
