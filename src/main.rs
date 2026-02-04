mod convert;
mod spawn;

use std::path::{Path, PathBuf};
use std::thread;

use clap::Parser;
use derive_more::Display;
use exn::{Exn, OptionExt, ResultExt, bail};
use thiserror::Error;
use tracing::{debug, error, info};

use crate::convert::{ArchivePath, ConversionConfig, Directory};

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
struct ErrorMessage(String);

fn single_archive(archive: ArchivePath, config: ConversionConfig) -> exn::Result<(), ErrorMessage> {
    let err = || {
        let msg = "Failed to create conversion job on a single archive".to_string();
        debug!("{msg}");
        ErrorMessage(msg)
    };

    info!("Checking {archive:?}");
    let job = match convert::SingleArchiveJob::new(archive, config).or_raise(err)? {
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

fn archives_in_dir(root: Directory, config: ConversionConfig) -> exn::Result<(), ErrorMessage> {
    let err = || {
        let msg = "Failed to convert all archives in a directory".to_string();
        debug!("{msg}");
        ErrorMessage(msg)
    };

    let jobs = convert::ArchivesInDirectoryJob::collect(root, config).or_raise(err)?;

    let bars = {
        let multi = indicatif::MultiProgress::new();
        let archives = multi.add(create_progress_bar("Archives"));
        let images = multi.add(create_progress_bar("Images"));

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

fn images_in_dir_recursively(
    root: Directory,
    config: ConversionConfig,
) -> exn::Result<(), ErrorMessage> {
    let err = || {
        let msg = "Failed to convert all images in a directory".to_string();
        debug!("{msg}");
        ErrorMessage(msg)
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

fn real_main() -> exn::Result<(), ErrorMessage> {
    let matches = Args::parse();

    if let Some(log_path) = matches.log {
        init_logger(&log_path, matches.level)?;
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

    let config = convert::ConversionConfig {
        target: matches.format,
        n_workers,
        forced: matches.force,
    };

    let path = matches.path;

    let (path, dir_exn) = match crate::convert::Directory::new(path) {
        Ok(root) => match matches.no_archive {
            true => return images_in_dir_recursively(root, config),
            false => return archives_in_dir(root, config),
        },
        Err(exn) => exn.recover(),
    };

    let (path, archive_exn) = match ArchivePath::new(path) {
        Ok(archive) => match matches.no_archive {
            true => {
                let (path, exn) = dir_exn.recover();
                let msg = format!("Got a file path when expecting a directory: {path:?}");
                debug!("{msg}");
                return Err(exn.raise(ErrorMessage(msg)));
            }
            false => return single_archive(archive, config),
        },
        Err(exn) => exn.recover(),
    };

    let msg = format!("Neither an archive nor a directory: {path:?}");
    debug!("{msg}");
    Err(Exn::raise_all(ErrorMessage(msg), [archive_exn, dir_exn]))
}

fn create_progress_bar(msg: &'static str) -> indicatif::ProgressBar {
    let style = indicatif::ProgressStyle::with_template(
        "[{elapsed_precise}] {msg:>9}: {wide_bar} {pos:>5}/{len:5}",
    )
    .unwrap();

    let bar = indicatif::ProgressBar::new(0)
        .with_style(style)
        .with_message(msg);
    bar.enable_responsive_tick(std::time::Duration::from_millis(250));
    bar
}

fn init_logger(path: &Path, level: tracing::Level) -> exn::Result<(), ErrorMessage> {
    let err = || {
        let msg = "Failed to initialize logging to file {path:?}".to_string();
        debug!("{msg}");
        ErrorMessage(msg)
    };

    let path = match path.is_absolute() {
        true => path,
        false => &PathBuf::from(".").join(path),
    };

    let directory = path.parent().ok_or_raise(err)?;

    if !directory.is_dir() {
        let msg = format!("Directory does not exist: {directory:?}");
        debug!("{msg}");
        let exn = Exn::from(ErrorMessage(msg)).raise(err());
        bail!(exn);
    }
    if path.exists() && !path.is_file() {
        let msg = format!("The path to the log file is not a regular file: {path:?}");
        debug!("{msg}");
        return Err(ErrorMessage(msg)).or_raise(err);
    }
    let Some(file_name) = path.file_name() else {
        let msg = format!("The filename is empty");
        debug!("{msg}");
        return Err(ErrorMessage(msg)).or_raise(err);
    };

    let writer = tracing_appender::rolling::never(directory, file_name);

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(writer)
        .with_ansi(false)
        .init();

    Ok(())
}

fn log_error(error: &Exn<ErrorMessage>) {
    fn walk(prefix: &str, frame: &exn::Frame) {
        let children = frame.children();
        if children.is_empty() {
            return;
        }

        let child_prefix = format!("{prefix}|   ");
        for (idx, frame) in children.iter().enumerate() {
            error!("{prefix}|-> {idx}: {}", frame.error());
            walk(&child_prefix, frame);
        }
    }

    error!("{error}");
    error!("Caused by:");

    walk("", error.frame());
}

fn main() -> exn::Result<(), ErrorMessage> {
    let ret = real_main();
    if let Err(e) = &ret {
        log_error(e);
    }
    ret
}
