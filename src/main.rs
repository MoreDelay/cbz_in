//! This programm is used to convert image files within Zip archives from one format to another.

use clap::Parser as _;
use tracing::error;

/// The program entry point.
///
/// It's only purpose is to log all errors bubbling up until here.
fn main() {
    let args = cbz_in::Args::parse();

    let ret = cbz_in::entry_point(args);

    match ret {
        Ok(()) => (),
        Err(exn) if cbz_in::got_interrupted(&exn) => {
            cbz_in::stderr("Got interrupted");
        }
        Err(exn) => {
            let report = cbz_in::CompactReport::new(&exn);
            println!("{report}");
            error!("Application error:\n{report:?}");
        }
    }
}
