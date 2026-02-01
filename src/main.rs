mod convert;
mod spawn;

use std::path::Path;
use std::path::PathBuf;
use std::thread;

use clap::Parser;
use derive_more::Display;
use exn::ResultExt;
use exn::bail;
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

#[derive(Debug, Display, Error)]
struct AppError(String);

fn single_archive(path: PathBuf, config: ConversionConfig) -> exn::Result<(), AppError> {
    let err = || {
        let msg = "Failed to run conversion on a single archive".to_string();
        AppError(msg)
    };

    info!("Converting {:?}", path);
    let job = match convert::SingleArchiveJob::new(path, config).or_raise(err)? {
        Ok(job) => job,
        Err(nothing_to_do) => {
            error!("{nothing_to_do}");
            println!("{nothing_to_do}");
            return Ok(());
        }
    };
    let inner_bar = create_progress_bar("Images");
    inner_bar.tick();
    job.run(&inner_bar).or_raise(err)?;
    inner_bar.finish();
    info!("Done");
    Ok(())
}

fn archives_in_dir(root: PathBuf, config: ConversionConfig) -> exn::Result<(), AppError> {
    let err = || {
        let msg = "Failed to convert all archives in a directory".to_string();
        AppError(msg)
    };

    let jobs = convert::ArchivesInDirectoryJob::collect(root, config).or_raise(err)?;

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

    jobs.run(&bars).or_raise(err)?;

    bars.images.finish();
    bars.archives.finish();
    Ok(())
}

fn images_in_dir_recursively(root: PathBuf, config: ConversionConfig) -> exn::Result<(), AppError> {
    let err = || {
        let msg = "Failed to convert all images in a directory".to_string();
        AppError(msg)
    };

    let job = match convert::RecursiveDirJob::new(root, config).or_raise(err)? {
        Ok(job) => job,
        Err(nothing_to_do) => {
            error!("{nothing_to_do}");
            println!("{nothing_to_do}");
            return Ok(());
        }
    };
    let inner_bar = create_progress_bar("Images");
    inner_bar.tick();
    job.run(&inner_bar).or_raise(err)?;
    inner_bar.finish();
    info!("Done");
    Ok(())
}

fn real_main() -> exn::Result<(), AppError> {
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
            true => {
                let msg = format!("Got a file path when expecting a directory: {path:?}");
                bail!(AppError(msg));
            }
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

fn create_logger(path: &Path) -> exn::Result<RollingFileAppender, AppError> {
    let err = || {
        let msg = "Failed to initialize logging to file {path:?}".to_string();
        AppError(msg)
    };

    let path = PathBuf::from(".").join(path);
    let directory = match path.parent() {
        Some(parent) => parent,
        None => Path::new("."),
    };
    if !directory.is_dir() {
        let msg = format!("Directory does not exist: {directory:?}");
        return Err(AppError(msg)).or_raise(err);
    }
    if path.exists() && !path.is_file() {
        let msg = format!("The path to the log file is not a regular file: {path:?}");
        return Err(AppError(msg)).or_raise(err);
    }
    let Some(file_name) = path.file_name() else {
        let msg = format!("The filename is empty");
        return Err(AppError(msg)).or_raise(err);
    };

    let writer = tracing_appender::rolling::never(directory, file_name);
    Ok(writer)
}

fn log_error(error: &exn::Exn<AppError>) {
    fn walk(prefix: &str, frame: &exn::Frame) {
        let children = frame.children();
        if children.is_empty() {
            return;
        }

        let child_prefix = format!("{prefix}    ");
        for (idx, frame) in children.iter().enumerate() {
            error!("{prefix}--> {idx}: {}", frame.error());
            walk(&child_prefix, frame);
        }
    }

    error!("{error}");
    error!("Caused by:");

    walk("", error.frame());
}

fn main() -> exn::Result<(), AppError> {
    let ret = real_main();
    if let Err(e) = &ret {
        log_error(e);
    }
    ret
}
