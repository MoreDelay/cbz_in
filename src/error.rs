//! Error structs used in this app.

use std::error::Error;

use derive_more::Display;
use exn::{Exn, Frame};
use tracing::debug;

/// General error object with a message for its context.
#[derive(Debug, Display)]
pub struct ErrorMessage(String);

impl Error for ErrorMessage {}

impl ErrorMessage {
    /// Create a error message by providing some context.
    pub fn new(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        debug!("{msg}");
        Self(msg)
    }
}

/// Context to why there is nothing to do for us.
#[derive(Debug, Display)]
pub struct NothingToDo(String);

impl Error for NothingToDo {}

impl NothingToDo {
    /// Create a message with an explanation.
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

/// Context to why there is nothing to do for us.
#[derive(Debug, Display)]
#[display("Got interrupted")]
pub struct Interrupted;

impl Error for Interrupted {}

/// Check if the error is caused by user interruptions.
pub fn got_interrupted(exn: &Exn<impl Error + Send + Sync>) -> bool {
    find_error::<Interrupted>(exn).is_some()
}

/// Walk the error context and find a specific error.
fn find_error<T: Error + 'static>(exn: &Exn<impl Error + Send + Sync>) -> Option<&T> {
    fn walk<T: Error + 'static>(frame: &Frame) -> Option<&T> {
        if let Some(e) = frame.error().downcast_ref::<T>() {
            return Some(e);
        }
        frame.children().iter().find_map(walk)
    }
    walk(exn.frame())
}
