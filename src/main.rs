mod convert;
mod spawn;

use std::ops::Deref;
use std::path::PathBuf;
use std::thread;

use clap::Parser;
use thiserror::Error;
use tracing::{debug, error, info};

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
}

#[derive(Error, Debug)]
enum AppError {
    #[error("Invalid archive path provided")]
    InvalidPath(#[from] convert::InvalidArchivePath),
    #[error("Error while handling an archive")]
    Archive(#[from] convert::ArchiveError),
    #[error("Could not walk the filesystem")]
    ReadingDir(#[source] std::io::Error),
}

fn real_main() -> Result<(), AppError> {
    let matches = Args::parse();
    let format = matches.format;
    let path = matches.path;

    let n_workers = match matches.workers {
        Some(Some(value)) => value,
        Some(None) => 1,
        None => match thread::available_parallelism() {
            Ok(value) => value.get(),
            Err(_) => 1,
        },
    };

    let force = matches.force;

    if path.is_dir() {
        for cbz_file in path.read_dir().map_err(AppError::ReadingDir)? {
            let cbz_file = match cbz_file {
                Ok(f) => f,
                Err(e) => {
                    error!("error while walking directory: {e}");
                    continue;
                }
            };
            let cbz_file = cbz_file.path();
            let conversion_file = match convert::ArchivePath::try_new(cbz_file) {
                Ok(f) => f,
                Err(e) => {
                    debug!("skipping: {e}");
                    continue;
                }
            };

            info!("Converting {:?}", conversion_file.deref());
            let job = match convert::ArchiveJob::new(conversion_file, format, n_workers, force)? {
                Ok(job) => job,
                Err(e) => {
                    info!("{e}");
                    continue;
                }
            };
            job.run()?;
            info!("Done");
        }
    } else {
        let conversion_file = convert::ArchivePath::try_new(path.clone())?;

        info!("Converting {:?}", conversion_file.deref());
        let job = match convert::ArchiveJob::new(conversion_file, format, n_workers, force)? {
            Ok(job) => job,
            Err(e) => {
                error!("{e}");
                return Ok(());
            }
        };
        job.run()?;
        info!("Done");
    }

    Ok(())
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
    tracing_subscriber::fmt::init();

    let ret = real_main();
    if let Err(e) = &ret {
        log_error(e);
    }
    Ok(ret?)
}
