//! Contains everything related to dealing with individual images.

use std::collections::VecDeque;
use std::io::BufRead;
use std::num::NonZeroUsize;
use std::{
    fs,
    path::{Path, PathBuf},
};

use exn::{ErrorExt, Exn, ResultExt};
use indicatif::ProgressBar;
use signal_hook::consts::{SIGCHLD, SIGINT};
use signal_hook::iterator::Signals;
use tracing::debug;

use crate::convert::Configuration;
use crate::{error::ErrorMessage, spawn};

/// Represents the task to convert an image from one type to another.
#[derive(Debug)]
pub struct ConversionJob {
    /// The current state of the job.
    state: State,
}

impl ConversionJob {
    /// Create a new conversion job that will follow the plan laid out in [Details].
    pub fn new(image_path: PathBuf, details: Details) -> Self {
        let waiting = StateWaiting {
            image_path,
            details,
            tool_use: ToolUse::Best,
        };
        let state = State::Waiting(waiting);
        Self { state }
    }

    /// Try to progress on this conversion job.
    ///
    /// As we need to wait for child processes to finish before advancing, we will not always
    /// succeed, which is encoded into the [Proceeded] return type.
    fn proceed(self) -> Result<Proceeded, Exn<ErrorMessage>> {
        let proceeded = match self.state {
            State::Waiting(waiting) => {
                let state = State::Running(waiting.start_conversion()?);
                Proceeded::Progress(Self { state })
            }
            State::Running(mut running) => match running.child_done()? {
                false => {
                    let state = State::Running(running);
                    Proceeded::SameAsBefore(Self { state })
                }
                true => {
                    let state = State::Completed(StateCompleted(running));
                    Proceeded::Progress(Self { state })
                }
            },
            State::Completed(completed) => match completed.complete() {
                Ok(Some(waiting)) => {
                    let state = State::Waiting(waiting);
                    Proceeded::Progress(Self { state })
                }
                Ok(None) => Proceeded::Finished,
                Err(exn) => {
                    let (waiting, exn) = exn.recover();
                    match waiting {
                        Some(waiting) => {
                            let state = State::Waiting(waiting);
                            Proceeded::Progress(Self { state })
                        }
                        None => return Err(exn),
                    }
                }
            },
        };
        Ok(proceeded)
    }
}

/// The return type for [ConversionJob::proceed] to indicate if we made progress.
#[derive(Debug)]
enum Proceeded {
    /// The job could not make any progress.
    SameAsBefore(ConversionJob),
    /// The job did make progress.
    Progress(ConversionJob),
    /// The job finished.
    Finished,
}

/// A representation of the inner state machine of [ConversionJob].
#[derive(Debug)]
enum State {
    /// We wait until a subprocess can be started.
    Waiting(StateWaiting),
    /// A child process is running for us.
    Running(StateRunning),
    /// Our child process has exited and we may clean up now.
    Completed(StateCompleted),
}

/// The data associated with the [State::Waiting].
#[derive(Debug)]
struct StateWaiting {
    /// The path to the image we want to convert.
    image_path: PathBuf,
    /// The conversion plan that remains to be executed for this image.
    details: Details,
    /// The type of tool we want to use for conversion.
    tool_use: ToolUse,
}

impl StateWaiting {
    /// State machine transition from [State::Waiting] to [State::Running].
    fn start_conversion(self) -> Result<StateRunning, Exn<ErrorMessage>> {
        let StateWaiting {
            image_path,
            details,
            tool_use,
        } = self;
        let err = || ErrorMessage::new(format!("Failed image conversion for {image_path:?}"));

        debug!("start conversion for {image_path:?}: {details:?}");

        let (from, to) = details.next_step();
        let input = &image_path;
        let output = &image_path.with_extension(to.ext());

        use ImageFormat::*;
        let child = match tool_use {
            ToolUse::Best => match (from, to) {
                (Jpeg, Png) => spawn::convert_jpeg_to_png(input, output).or_raise(err)?,
                (Png, Jpeg) => spawn::convert_png_to_jpeg(input, output).or_raise(err)?,
                (Jpeg | Png, Avif) => spawn::encode_avif(input, output).or_raise(err)?,
                (Jpeg | Png, Jxl) => spawn::encode_jxl(input, output).or_raise(err)?,
                (Jpeg | Png, Webp) => spawn::encode_webp(input, output).or_raise(err)?,
                (Avif, Jpeg) => spawn::decode_avif_to_jpeg(input, output).or_raise(err)?,
                (Avif, Png) => spawn::decode_avif_to_png(input, output).or_raise(err)?,
                (Jxl, Jpeg) => spawn::decode_jxl_to_jpeg(input, output).or_raise(err)?,
                (Jxl, Png) => spawn::decode_jxl_to_png(input, output).or_raise(err)?,
                (Webp, Png) => spawn::decode_webp(input, output).or_raise(err)?,
                (_, _) => unreachable!(),
            },
            ToolUse::Backup { .. } => match spawn::convert_with_magick(input, output) {
                Ok(child) => child,
                Err(exn) => {
                    let last_exn = tool_use
                        .get_exn()
                        .expect("checked by match that we can get exn");
                    let exn = Exn::raise_all(err(), [last_exn, exn]);
                    return Err(exn);
                }
            },
        };

        Ok(StateRunning {
            child,
            image_path,
            details,
            tool_use,
        })
    }
}

/// The data associated with the [State::Running].
#[derive(Debug)]
struct StateRunning {
    /// The tracked child process.
    child: spawn::ManagedChild,
    /// The original image path that is currently being converted.
    image_path: PathBuf,
    /// The conversion plan that remains to be executed for this image.
    details: Details,
    /// The type of tool we want to use for conversion.
    tool_use: ToolUse,
}

impl StateRunning {
    /// Check if the process has already completed.
    fn child_done(&mut self) -> Result<bool, Exn<ErrorMessage>> {
        let err = || {
            let path = &self.image_path;
            ErrorMessage::new(format!(
                "Could not check if a process finished working on {path:?}",
            ))
        };
        self.child.try_wait().or_raise(err)
    }
}

/// The data associated with the [State::Completed].
///
/// This is closer to a pseudo state between Running and Waiting. Its purpose is to perform the
/// final cleanup after the child process has completed the conversion.
#[derive(Debug)]
struct StateCompleted(StateRunning);

impl StateCompleted {
    /// State machine transition from [State::Completed] to [State::Waiting].
    ///
    /// Waits on the child process and deletes the original image file. If we completed all
    /// conversions as specified by the details, we stop here.
    fn complete(self) -> Result<Option<StateWaiting>, Exn<ErrorMessage, Option<StateWaiting>>> {
        let Self(StateRunning {
            child,
            image_path,
            details,
            tool_use,
        }) = self;

        debug!("completed conversion for {image_path:?}: {details:?}");

        if let Err(exn) = child.wait() {
            let exn = match Self::try_to_recover(exn, image_path, details, tool_use) {
                Ok(exn) => exn.map(Some),
                Err(exn) => exn.attach(None),
            };
            return Err(exn);
        }
        // at this point we have successfully converted the image and prepare the next conversion
        drop(tool_use);

        let err = || {
            let path = &image_path;
            ErrorMessage::new(format!("Could not complete a conversion for {path:?}"))
        };

        fs::remove_file(&image_path).or_raise_with_recovery(err, None)?;

        let after = match details {
            Details::OneStep { .. } | Details::Finish { .. } => None,
            Details::TwoStep { over: from, to, .. } => {
                let waiting = StateWaiting {
                    image_path: image_path.with_extension(from.ext()),
                    details: Details::Finish { from, to },
                    tool_use: ToolUse::Best,
                };
                Some(waiting)
            }
        };
        Ok(after)
    }

    /// Fall back to an alternative conversion plan when the dedicated tool failed.
    ///
    /// When the child process existed abnormally, we can try to recover the conversion process
    /// using `magick`, as it is more forgiving for out-of-spec files than other tools.
    fn try_to_recover(
        exn: Exn<ErrorMessage>,
        image_path: PathBuf,
        details: Details,
        tool_use: ToolUse,
    ) -> Result<Exn<ErrorMessage, StateWaiting>, Exn<ErrorMessage>> {
        if let ToolUse::Backup { last_error } = tool_use {
            let msg = format!("Give up trying to convert {image_path:?}");
            let err = ErrorMessage::new(msg);
            return Err(Exn::raise_all(err, [last_error, exn]));
        }

        let details = match details {
            Details::OneStep { from, to } | Details::TwoStep { from, to, .. } => Details::TwoStep {
                from,
                over: ImageFormat::Png,
                to,
            },
            Details::Finish { .. } => {
                let msg = "Image from a previous pass could not be formatted further, \
                                something is gravely wrong";
                return Err(exn.raise(ErrorMessage::new(msg)));
            }
        };

        let msg = format!("Could not complete the conversion for {image_path:?}, try to recover");

        let waiting = StateWaiting {
            image_path,
            details,
            tool_use: ToolUse::Backup { last_error: exn },
        };

        Ok(Exn::with_recovery(ErrorMessage::new(msg), waiting))
    }
}

/// Represents the plan of the conversion sequence an image will go through.
#[derive(Debug)]
pub enum Details {
    /// A single step conversion.
    OneStep {
        /// Initial image format.
        from: ImageFormat,
        /// Target image format.
        to: ImageFormat,
    },
    /// The first step in a two-step conversion.
    TwoStep {
        /// Initial image format.
        from: ImageFormat,
        /// Intermediate image format.
        over: ImageFormat,
        /// Target image format.
        to: ImageFormat,
    },
    /// The second step in a two-step conversion
    Finish {
        /// Initial image format.
        from: ImageFormat,
        /// Target image format.
        to: ImageFormat,
    },
}

impl Details {
    /// Determine the details for a specific image to reach the goal set out by the configuration.
    pub fn new(
        image_path: &Path,
        current: ImageFormat,
        config: &Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        use ImageFormat::*;

        let err = || {
            let current = current.ext();
            let target = config.target.ext();
            ErrorMessage::new(format!(
                "Failed to create the conversion job from {current:?} to {target:?} for {image_path:?}",
            ))
        };

        let &Configuration { target, forced, .. } = config;

        let out = match (current, target) {
            (a, b) if a == b => return Ok(None),
            (Avif, Jxl | Webp) => Self::TwoStep {
                from: current,
                over: Png,
                to: target,
            },
            (Jxl, Avif | Webp) => match Self::jxl_is_compressed_jpeg(image_path).or_raise(err)? {
                true => Self::TwoStep {
                    from: current,
                    over: Jpeg,
                    to: target,
                },
                false => Self::TwoStep {
                    from: current,
                    over: Png,
                    to: target,
                },
            },
            (Webp, Jpeg | Avif | Jxl) => Self::TwoStep {
                from: current,
                over: Png,
                to: target,
            },
            (_, _) => Self::OneStep {
                from: current,
                to: target,
            },
        };
        let perform = forced || out.perform_always();
        Ok(perform.then_some(out))
    }

    /// Check if these details will always be performed without need of [Configuration::forced].
    fn perform_always(&self) -> bool {
        let tuple = match self {
            Details::OneStep { from, to, .. } => (*from, *to),
            Details::TwoStep { from, to, .. } => (*from, *to),
            Details::Finish { .. } => return true,
        };

        use ImageFormat::*;
        match tuple {
            (Jpeg | Png, _) => true,
            (_, Jpeg | Png) => true,
            (_, _) => false,
        }
    }

    /// Returns the conversion sequence that should be performed next in the current plan.
    fn next_step(&self) -> (ImageFormat, ImageFormat) {
        match *self {
            Details::OneStep { from, to } => (from, to),
            Details::TwoStep { from, over, .. } => (from, over),
            Details::Finish { from, to } => (from, to),
        }
    }

    /// Check if a Jxl file is actually a re-encoded Jpeg.
    ///
    /// If that is the case, then we would prefer to simple decode it again, instead of routing
    /// over Png.
    fn jxl_is_compressed_jpeg(image_path: &Path) -> Result<bool, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new(format!("Could not query jxl file {image_path:?}"));

        let has_box = spawn::run_jxlinfo(image_path)
            .and_then(|c| c.wait_with_output())
            .or_raise(err)?
            .stdout
            .lines()
            .any(|line| line.unwrap().starts_with("box: type: \"jbrd\""));
        Ok(has_box)
    }
}

/// Indicates what tool to use for the next conversion.
///
/// While we prefer to use dedicated tools for all conversions, these tools may sometimes fail on
/// files that do not follow the image file specification fully. In that case, we fall back to try
/// with `magick`. Only if that also fails do we report an issue.
#[derive(Debug)]
enum ToolUse {
    /// Use the intended, dedicated tool.
    Best,
    /// We failed before, so try to use `magick` now.
    Backup {
        /// The error context from the last failure
        last_error: Exn<ErrorMessage>,
    },
}

impl ToolUse {
    /// Extract the error context from the previous failure.
    fn get_exn(self) -> Option<Exn<ErrorMessage>> {
        match self {
            ToolUse::Best => None,
            ToolUse::Backup { last_error } => Some(last_error),
        }
    }
}

/// A collection of image conversion jobs.
///
/// This is the dedicated collection of jobs that only handles images. It is the task of other jobs
/// to prepare the image files in the filesystem such that all operations are non-destructive to
/// the original files.
pub struct ConversionJobs {
    /// A queue of jobs waiting to be run.
    job_queue: VecDeque<ConversionJob>,
    /// Jobs we are currently actively progressing.
    jobs_in_progress: Vec<Option<ConversionJob>>,
}

impl ConversionJobs {
    /// Initialize a new collection with the given jobs.
    pub fn new(job_queue: VecDeque<ConversionJob>, n_workers: NonZeroUsize) -> Self {
        let jobs_in_progress = Vec::from_iter((0..n_workers.get()).map(|_| None));
        Self {
            job_queue,
            jobs_in_progress,
        }
    }

    /// Run this job.
    pub fn run(mut self, bar: &ProgressBar) -> Result<(), Exn<ErrorMessage>> {
        assert!(!self.job_queue.is_empty());
        bar.reset();
        bar.set_length(self.job_queue.len() as u64);

        // these signals will be catched from here on out until dropped
        let mut signals = Signals::new([SIGINT, SIGCHLD])
            .or_raise(|| ErrorMessage::new("Could not listen to process signals"))?;

        // start out as many jobs as allowed
        for slot in self.jobs_in_progress.iter_mut() {
            let Some(job) = self.job_queue.pop_front() else {
                break;
            };
            *slot = Some(job);
        }
        self.proceed_jobs(bar)?;

        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => {
                        return Err(ErrorMessage::new("Got interrupted").raise());
                    }
                    SIGCHLD => self.proceed_jobs(bar)?,
                    _ => unreachable!(),
                }
            }
        }
        Ok(())
    }

    /// Try to proceed as many jobs as possible, until none are doing any more progress.
    fn proceed_jobs(&mut self, bar: &ProgressBar) -> Result<(), Exn<ErrorMessage>> {
        for slot in self.jobs_in_progress.iter_mut() {
            loop {
                match slot.take() {
                    Some(job) => match job.proceed()? {
                        Proceeded::SameAsBefore(job) => {
                            *slot = Some(job);
                            break;
                        }
                        Proceeded::Progress(job) => *slot = Some(job),
                        Proceeded::Finished => bar.inc(1),
                    },
                    None => match self.job_queue.pop_front() {
                        Some(job) => *slot = Some(job),
                        None => break,
                    },
                }
            }
        }
        Ok(())
    }

    /// Check if there are any jobs currently in progress.
    fn jobs_pending(&self) -> bool {
        self.jobs_in_progress.iter().any(|job| job.is_some())
    }
}

/// All supported image file formats.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum ImageFormat {
    /// A JPEG file.
    Jpeg,
    /// A PNG file.
    Png,
    /// A AVIF file.
    Avif,
    /// A JXL file.
    Jxl,
    /// A WebP file.
    Webp,
}

impl ImageFormat {
    /// Get the file extension as string.
    pub fn ext(self) -> &'static str {
        use ImageFormat::*;

        match self {
            Jpeg => "jpeg",
            Png => "png",
            Avif => "avif",
            Jxl => "jxl",
            Webp => "webp",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_for_compressed_jxl() {
        let compressed_path = PathBuf::from("test_data/compressed.jxl");
        assert!(compressed_path.exists());
        let out = Details::jxl_is_compressed_jpeg(&compressed_path).unwrap();
        assert!(out);
    }

    #[test]
    fn test_check_for_encoded_jxl() {
        let encoded_path = PathBuf::from("test_data/encoded.jxl");
        assert!(encoded_path.exists());
        let out = Details::jxl_is_compressed_jpeg(&encoded_path).unwrap();
        assert!(!out);
    }
}
