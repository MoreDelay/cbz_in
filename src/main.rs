mod convert;
mod spawn;

use std::path::Path;
use std::path::PathBuf;
use std::thread;

use clap::Parser;
use thiserror::Error;
use tracing::{error, info};
use tracing_appender::rolling::RollingFileAppender;

/// Convert images within comic archives to newer image formats
///
/// Convert images within Zip Comic Book archives, although it also works with normal zip files.
/// By default only converts Jpeg and Png to the target format or decode any formats to Png and
/// Jpeg.
#[derive(Parser)]
#[command(version, verbatim_doc_comment)]
struct Args {
    #[arg(
        required = true,
        help = "All images within the archive(s) are converted to this format"
    )]
    format: convert::ImageFormat,

    #[arg(
        default_value = ".",
        help = "Path to a cbz file or a directory containing cbz files"
    )]
    path: PathBuf,

    /// Number of processes spawned
    ///
    /// Uses as many processes as you have cores by default.
    /// When used as a flag only spawns a single process at a time.
    #[arg(short = 'j', long, verbatim_doc_comment)]
    workers: Option<Option<usize>>,

    #[arg(short, long, help = "Convert all images of all formats")]
    force: bool,

    #[arg(
        long,
        num_args(0..=1),
        default_missing_value = "./cbz_in.log",
        help = "Write a log file"
    )]
    log: Option<PathBuf>,

    #[arg(long, default_value = "info", help = "Level of logging")]
    level: tracing::Level,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("Error when trying to log")]
    Logging(#[from] LoggingError),
    #[error("Error when trying to print the progress")]
    Printing(#[from] std::io::Error),
    #[error("Error while collecting archives from root directory '{0}'")]
    CollectArchives(PathBuf, #[source] convert::ArchiveJobsError),
    #[error("Invalid archive path provided")]
    InvalidPath(#[from] convert::InvalidArchivePath),
    #[error("Error while handling an archive")]
    Archive(#[from] convert::ArchiveError),
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
        let jobs = convert::ArchiveJobs::collect(&path, config)
            .map_err(|e| AppError::CollectArchives(path.to_path_buf(), e))?;
        let n = jobs.len();

        let bars = indicatif::MultiProgress::new();
        let archive_bar = bars.add(create_progress_bar("Archive".to_string(), Some(n as u64)));
        let inner_bar = bars.add(create_progress_bar("Images".to_string(), None));

        archive_bar.tick();
        inner_bar.tick();

        for job in jobs.into_iter() {
            info!("Converting {:?}", job.archive());
            bars.println(format!("Converting {:?}", job.archive()))?;
            job.run(&inner_bar)?;
            archive_bar.inc(1);
            info!("Done");
        }
        inner_bar.finish();
        archive_bar.finish();
    } else {
        info!("Converting {:?}", path);
        let job = match convert::ArchiveJob::new(path, config)? {
            Ok(job) => job,
            Err(nothing_to_do) => {
                error!("{nothing_to_do}");
                println!("{nothing_to_do}");
                return Ok(());
            }
        };
        let inner_bar = create_progress_bar("Images".to_string(), None);
        inner_bar.tick();
        job.run(&inner_bar)?;
        info!("Done");
    }

    Ok(())
}

fn create_progress_bar(msg: String, len: Option<u64>) -> indicatif::ProgressBar {
    let style = indicatif::ProgressStyle::with_template(
        "[{elapsed_precise}] {msg:>8}: {wide_bar} {pos:>5}/{len:5}",
    )
    .unwrap();

    indicatif::ProgressBar::new(len.unwrap_or(0))
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
    if source.is_some() {
        error!("Caused by:");
    }
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
