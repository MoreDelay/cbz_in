use derive_more::Display;
use tracing::debug;

#[derive(Debug, Display)]
pub struct ErrorMessage(String);

impl std::error::Error for ErrorMessage {}

impl ErrorMessage {
    pub fn new(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        debug!("{msg}");
        Self(msg)
    }
}

#[derive(Debug, Display)]
pub struct NothingToDo(String);

impl std::error::Error for NothingToDo {}

impl NothingToDo {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}
