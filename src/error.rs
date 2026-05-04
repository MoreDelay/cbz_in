//! Error structs used in this app.

use std::{error::Error, marker::PhantomData};

use derive_more::Display;
use exn::{Exn, Frame};
use tracing::error;

/// General error object with a message for its context.
pub struct Msg<T>(String, PhantomData<T>);

// Safety: `T` is only used as in a phantom field, and `String` is Send + Sync
unsafe impl<T> Send for Msg<T> {}
// Safety: `T` is only used as in a phantom field, and `String` is Send + Sync
unsafe impl<T> Sync for Msg<T> {}

impl<T> std::fmt::Display for Msg<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<T> std::fmt::Debug for Msg<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ErrorMessage").field(&self.0).finish()
    }
}

impl<T> Error for Msg<T> {}

impl Msg<()> {
    /// Create a tag-less error message by providing some context.
    pub fn no_tag(msg: impl Into<String>) -> Self {
        Self::new(msg)
    }
}

impl<T> Msg<T> {
    /// Create a type checked error message by providing some context.
    pub fn new(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        error!("{msg}");
        Self(msg, PhantomData)
    }
}

/// Context to why there is nothing to do for us.
pub struct NothingToDo<T> {
    /// The original location of the image container.
    pub path: T,
    /// The reason why there is nothing to do.
    pub reason: NothingToDoReason,
}

/// The reason why we decided there is nothing to do.
#[derive(Debug)]
pub enum NothingToDoReason {
    /// We found the conversion was performed before.
    AlreadyConverted,
    /// The requested conversion is not applicable to any image found.
    NothingToConvert,
}

impl std::fmt::Display for NothingToDoReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyConverted => f.write_str("Already converted"),
            Self::NothingToConvert => f.write_str("Nothing to convert"),
        }
    }
}

/// Context to why there is nothing to do for us.
#[derive(Debug, Display)]
#[display("Got interrupted")]
pub struct Interrupted;

impl Error for Interrupted {}

/// Check if the error is caused by user interruptions.
#[must_use]
pub fn got_interrupted(exn: &Exn<impl Error + Send + Sync>) -> bool {
    find_error::<Interrupted>(exn).is_some()
}

/// Walk the error context and find a specific error.
fn find_error<E: Error + 'static>(exn: &Exn<impl Error + Send + Sync>) -> Option<&E> {
    fn walk<E: Error + 'static>(frame: &Frame) -> Option<&E> {
        if let Some(e) = frame.error().downcast_ref() {
            return Some(e);
        }
        frame.children().iter().find_map(walk)
    }
    walk(exn.frame())
}

/// Wrapper around the Exn error context to implement custom formatting.
pub struct CompactReport<'a, E>(pub &'a Exn<E>)
where
    E: Error + Send + Sync + 'static;

impl<E: Error + Send + Sync + 'static> std::fmt::Display for CompactReport<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const DEBUG: bool = false;
        Self::format(f, self.0.frame(), "", DEBUG)
    }
}

impl<E: Error + Send + Sync + 'static> std::fmt::Debug for CompactReport<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const DEBUG: bool = true;
        Self::format(f, self.0.frame(), "", DEBUG)
    }
}

impl<'a, E> CompactReport<'a, E>
where
    E: Error + Send + Sync + 'static,
{
    /// Create a new reporting wrapper.
    #[must_use]
    pub const fn new(exn: &'a Exn<E>) -> Self {
        Self(exn)
    }

    /// Write out an error report in a compacter format than default.
    fn format(
        f: &mut std::fmt::Formatter<'_>,
        frame: &Frame,
        prefix: &str,
        debug: bool,
    ) -> std::fmt::Result {
        match debug {
            true => Self::debug_line(f, frame)?,
            false => Self::display_line(f, frame)?,
        }

        match frame.children() {
            [] => (),
            [child] => {
                write!(f, "\n{prefix}-> ")?;
                Self::format(f, child, prefix, debug)?;
            }
            children => {
                let child_prefix = format!("{prefix}   ");
                for child in children {
                    write!(f, "\n{prefix}|> ")?;
                    Self::format(f, child, &child_prefix, debug)?;
                }
            }
        }
        Ok(())
    }

    /// Write the error for this frame with debug information.
    fn debug_line(f: &mut std::fmt::Formatter<'_>, frame: &Frame) -> std::fmt::Result {
        let loc = frame.location();
        write!(
            f,
            "[{}:{}:{}] {}",
            loc.file(),
            loc.line(),
            loc.column(),
            frame.error()
        )
    }

    /// Write the error for this frame without debug information.
    fn display_line(f: &mut std::fmt::Formatter<'_>, frame: &Frame) -> std::fmt::Result {
        write!(f, "{}", frame.error())
    }
}
