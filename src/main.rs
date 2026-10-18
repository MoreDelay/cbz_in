//! This program is used to convert image files within Zip archives from one format to another.

use std::process::ExitCode;

use clap::Parser as _;
use tracing::{error, info};

/// The program entry point.
///
/// Its only purpose is to log all errors bubbling up until here.
fn main() -> ExitCode {
    let args = cbz_in::Args::parse();

    // activate logs before entering app
    if !args.no_log
        && let Err(exn) = cbz_in::init_logger(&args.log_path, args.level)
    {
        let report = cbz_in::CompactReport::new(&exn);
        eprintln!("{report}");
        error!("Application error:\n{report:?}");
        return ExitCode::FAILURE;
    }

    // Log how we got invoked
    let cmd = std::env::args_os()
        .map(|s| s.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    info!("starting new execution as {cmd:?}");
    let cwd = std::env::current_dir().unwrap_or_default();
    info!("working directory: {:?}", cwd);

    // The actual execution starts here
    let ret = cbz_in::entry_point(args);

    match ret {
        Ok(cbz_in::AppExit::Success) => ExitCode::SUCCESS,
        Ok(cbz_in::AppExit::NothingFound) => ExitCode::FAILURE,
        Err(exn) if cbz_in::got_interrupted(&exn) => {
            let msg = "Got interrupted";
            eprintln!("{msg}");
            error!("{msg}");
            ExitCode::FAILURE
        }
        Err(exn) => {
            let report = cbz_in::CompactReport::new(&exn);
            eprintln!("{report}");
            error!("Application error:\n{report:?}");
            ExitCode::FAILURE
        }
    }
}
