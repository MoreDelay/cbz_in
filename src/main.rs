//! This program is used to convert image files within Zip archives from one format to another.

use std::process::ExitCode;

use clap::Parser as _;
use tracing::error;

/// The program entry point.
///
/// Its only purpose is to log all errors bubbling up until here.
fn main() -> ExitCode {
    let args = cbz_in::Args::parse();

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
