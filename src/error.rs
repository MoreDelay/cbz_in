//! Error structs used in this app.

use derive_more::Display;
use tracing::debug;

/// General error object with a message for its context.
#[derive(Debug, Display)]
pub struct ErrorMessage(String);

impl std::error::Error for ErrorMessage {}

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

impl std::error::Error for NothingToDo {}

impl NothingToDo {
    /// Create a message with an explanation.
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}
