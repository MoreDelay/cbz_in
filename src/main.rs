mod convert;
mod spawn;

use std::path::Path;
use std::path::PathBuf;
use std::thread;

use clap::Parser;
use thiserror::Error;
use tracing::{error, info};
use tracing_appender::rolling::RollingFileAppender;

use crate::convert::ConversionConfig;

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
    format: convert::ImageFormat,

    /// Path to a cbz file or a directory containing cbz files.
    #[arg(default_value = ".", verbatim_doc_comment)]
    path: PathBuf,

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

#[derive(Error, Debug)]
enum AppError {
    #[error("Error when trying to log")]
    Logging(#[from] LoggingError),
    #[error("Got a file path when expecting a directory: '{0}'")]
    ExpectDir(PathBuf),
    #[error("Error while handling an archive")]
    SingleArchive(#[from] convert::SingleArchiveJobError),
    #[error("Error while converting archives in '{0}'")]
    ArchivesInDir(PathBuf, #[source] convert::ArchivesInDirectoryJobError),
    #[error("Error converting images recursively")]
    RecursiveDir(#[from] convert::RecursiveDirJobError),
}

fn single_archive(path: PathBuf, config: ConversionConfig) -> Result<(), AppError> {
    info!("Converting {:?}", path);
    let job = match convert::SingleArchiveJob::new(path, config)? {
        Ok(job) => job,
        Err(nothing_to_do) => {
            error!("{nothing_to_do}");
            println!("{nothing_to_do}");
            return Ok(());
        }
    };
    let inner_bar = create_progress_bar("Images");
    inner_bar.tick();
    job.run(&inner_bar)?;
    inner_bar.finish();
    info!("Done");
    Ok(())
}

fn archives_in_dir(root: PathBuf, config: ConversionConfig) -> Result<(), AppError> {
    let jobs = convert::ArchivesInDirectoryJob::collect(&root, config)
        .map_err(|e| AppError::ArchivesInDir(root.to_path_buf(), e))?;

    let bars = {
        let multi = indicatif::MultiProgress::new();
        let archives = multi.add(create_progress_bar("Archives"));
        let images = multi.add(create_progress_bar("Images"));

        archives.tick();
        images.tick();

        convert::Bars {
            multi,
            archives,
            images,
        }
    };

    jobs.run(&bars)
        .map_err(|e| AppError::ArchivesInDir(root, e))?;

    bars.images.finish();
    bars.archives.finish();
    Ok(())
}

fn images_in_dir_recursively(root: PathBuf, config: ConversionConfig) -> Result<(), AppError> {
    let job = match convert::RecursiveDirJob::new(root, config)? {
        Ok(job) => job,
        Err(nothing_to_do) => {
            error!("{nothing_to_do}");
            println!("{nothing_to_do}");
            return Ok(());
        }
    };
    let inner_bar = create_progress_bar("Images");
    inner_bar.tick();
    job.run(&inner_bar)?;
    inner_bar.finish();
    info!("Done");
    Ok(())
}

fn real_main() -> Result<(), AppError> {
    let matches = Args::parse();

    if let Some(log_path) = matches.log {
        let writer = create_logger(&log_path)?;
        tracing_subscriber::fmt()
            .with_writer(writer)
            .with_ansi(false)
            .init();
    }

    let n_workers = match matches.workers {
        Some(Some(value)) => value,
        Some(None) => 1,
        None => match thread::available_parallelism() {
            Ok(value) => value.get(),
            Err(_) => 1,
        },
    };

    let config = convert::ConversionConfig {
        target: matches.format,
        n_workers,
        forced: matches.force,
    };

    let path = matches.path;
    if path.is_dir() {
        match matches.no_archive {
            true => images_in_dir_recursively(path, config),
            false => archives_in_dir(path, config),
        }
    } else {
        match matches.no_archive {
            true => Err(AppError::ExpectDir(path.to_path_buf())),
            false => single_archive(path, config),
        }
    }
}

fn create_progress_bar(msg: &'static str) -> indicatif::ProgressBar {
    let style = indicatif::ProgressStyle::with_template(
        "[{elapsed_precise}] {msg:>9}: {wide_bar} {pos:>5}/{len:5}",
    )
    .unwrap();

    indicatif::ProgressBar::new(0)
        .with_style(style)
        .with_message(msg)
}

#[derive(Debug, Error)]
enum LoggingError {
    #[error("Directory for log file does not exist: '{0}'")]
    DirNotExist(PathBuf),
    #[error("The path to the log file is not a regular file: '{0}")]
    FileNotNormal(PathBuf),
    #[error("The path to the log file is missing its file name: '{0}'")]
    MissingName(PathBuf),
}

fn create_logger(path: &Path) -> Result<RollingFileAppender, LoggingError> {
    let directory = match path.parent() {
        Some(parent) => parent,
        None => Path::new("."),
    };
    if !directory.is_dir() {
        return Err(LoggingError::DirNotExist(directory.to_path_buf()));
    }
    if path.exists() && !path.is_file() {
        return Err(LoggingError::FileNotNormal(path.to_path_buf()));
    }
    let Some(file_name) = path.file_name() else {
        return Err(LoggingError::MissingName(path.to_path_buf()));
    };

    let writer = tracing_appender::rolling::never(directory, file_name);
    Ok(writer)
}

fn log_error(error: &dyn std::error::Error) {
    error!("{error}");
    let mut source = error.source();
    if source.is_none() {
        return;
    }
    error!("Caused by:");
    let mut counter = 0;
    while let Some(error) = source {
        error!("    {counter}: {error}");
        source = error.source();
        counter += 1;
    }
}

fn main() -> anyhow::Result<()> {
    let ret = real_main();
    if let Err(e) = &ret {
        log_error(e);
    }
    Ok(ret?)
}
