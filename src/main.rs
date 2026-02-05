mod convert;
mod error;
mod spawn;

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::thread;

use clap::Parser;
use exn::{ErrorExt, Exn, OptionExt, ResultExt};
use tracing::{error, info};

use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
use crate::convert::{ImageFormat, JobCollection};
use crate::error::ErrorMessage;

fn main() -> Result<(), Exn<ErrorMessage>> {
    let ret = real_main();
    if let Err(e) = &ret {
        error!("Application error:\n{e:?}");
    }
    ret
}

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

    let n_workers = match matches.workers {
        Some(Some(value)) => value,
        Some(None) => 1,
        None => match thread::available_parallelism() {
            Ok(value) => value.get(),
            Err(_) => 1,
        },
    };

    let config = convert::Configuration {
        target: matches.format,
        n_workers,
        forced: matches.force,
    };

    let paths = VecDeque::from(matches.paths);

    let main_job = match matches.no_archive {
        true => MainJob::collect_directory_jobs(paths, config).or_raise(err)?,
        false => MainJob::collect_archive_jobs(paths, config).or_raise(err)?,
    };
    let Some(main_job) = main_job else {
        eprintln!("Nothing to do");
        return Ok(());
    };
    main_job.run().or_raise(err)
}

/// Convert images within comic archives to newer image formats.
///
/// Convert images within Comic Book Zip (CBZ) archives, although it also works with normal zip
/// files. By default only converts Jpeg and Png to the target format or decode any formats to
/// Png and Jpeg. The new archive with converted images is placed adjacent to the original, so this
/// operation is non-destructive.
#[derive(Parser)]
#[command(version, verbatim_doc_comment)]
struct Args {
    /// All images within the archive(s) are converted to this format.
    #[arg(required = true, verbatim_doc_comment)]
    format: ImageFormat,

    /// Path to a cbz file or a directory containing cbz files.
    #[arg(default_value = ".", num_args = 1.., verbatim_doc_comment)]
    paths: Vec<PathBuf>,

    /// Number of processes spawned.
    ///
    /// Uses as many processes as you have cores by default. When used as a flag only spawns a
    /// single process at a time.
    #[arg(short = 'j', long, verbatim_doc_comment)]
    workers: Option<Option<usize>>,

    /// Convert all images of all formats.
    #[arg(short, long, verbatim_doc_comment)]
    force: bool,

    /// Convert images in the directory directly (recursively).
    ///
    /// This will create a copy of your directory structure using hard links. This means your data
    /// is not copied as both structures point to the same underlying files. The only difference
    /// between both directory structures are the converted images found in a recursive search.
    #[arg(long, verbatim_doc_comment)]
    no_archive: bool,

    /// Write a log file.
    #[arg(
        long ,
        value_name = "LOG_FILE",
        num_args(0..=1),
        default_missing_value = "./cbz_in.log",
        verbatim_doc_comment
    )]
    log: Option<PathBuf>,

    /// Detail level of logging.
    #[arg(long, default_value = "info", verbatim_doc_comment)]
    level: tracing::Level,
}

enum MainJob {
    Archives(convert::ArchiveJobs),
    Directories(convert::RecursiveDirJobs),
}

impl MainJob {
    fn collect_archive_jobs(
        paths: VecDeque<PathBuf>,
        config: convert::Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path) {
                Ok(root) => return Ok(Some(archives_in_dir(root, config)?)),
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
        let jobs = convert::ArchiveJobs::new(jobs).map(Self::Archives);
        Ok(jobs)
    }

    fn collect_directory_jobs(
        paths: VecDeque<PathBuf>,
        config: convert::Configuration,
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
        let jobs = convert::RecursiveDirJobs::new(jobs).map(Self::Directories);
        Ok(jobs)
    }

    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        let collection_type = match self {
            MainJob::Archives(_) => convert::CollectionType::Archives,
            MainJob::Directories(_) => convert::CollectionType::Directories,
        };
        let bars = convert::Bars::new(collection_type);

        let res = match self {
            MainJob::Archives(jobs) => jobs.run(&bars),
            MainJob::Directories(jobs) => jobs.run(&bars),
        };

        bars.jobs().finish();
        bars.images().finish();
        res
    }
}

fn single_archive(
    archive: ArchivePath,
    config: convert::Configuration,
) -> Result<Option<convert::ArchiveJobs>, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to create conversion job on a single archive");

    info!("Checking archive {archive:?}");
    convert::ArchiveJobs::single(archive, config).or_raise(err)
}

fn archives_in_dir(
    root: Directory,
    config: convert::Configuration,
) -> Result<convert::ArchiveJobs, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to convert all archives in a directory");

    info!("Checking archives directory {root:?}");
    convert::ArchiveJobs::collect(root, config).or_raise(err)
}

fn images_in_dir_recursively(
    root: Directory,
    config: convert::Configuration,
) -> Result<Option<convert::RecursiveDirJobs>, Exn<ErrorMessage>> {
    let err = || ErrorMessage::new("Failed to convert all images in a directory");

    info!("Checking root directory {root:?}");
    convert::RecursiveDirJobs::single(root, config).or_raise(err)
}

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
        let msg = "The filename is empty".to_string();
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
