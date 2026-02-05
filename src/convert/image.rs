use std::collections::VecDeque;
use std::io::BufRead;
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

#[derive(Debug)]
pub enum ConversionJob {
    Waiting(ConversionJobWaiting),
    Running(ConversionJobRunning),
    Completed(ConversionJobCompleted),
}

impl ConversionJob {
    pub fn new(image_path: PathBuf, details: ConversionJobDetails) -> Self {
        let waiting = ConversionJobWaiting {
            image_path,
            details,
            tool_use: ToolUse::Best,
        };
        Self::Waiting(waiting)
    }

    pub fn proceed(self) -> Result<Proceeded, Exn<ErrorMessage>> {
        let proceeded = match self {
            ConversionJob::Waiting(waiting) => {
                let running = waiting.start_conversion()?;
                Proceeded::Progress(Self::Running(running))
            }
            ConversionJob::Running(mut running) => match running.child_done()? {
                false => Proceeded::SameAsBefore(Self::Running(running)),
                true => {
                    let completed = ConversionJobCompleted(running);
                    Proceeded::Progress(Self::Completed(completed))
                }
            },
            ConversionJob::Completed(completed) => match completed.complete() {
                Ok(Some(waiting)) => Proceeded::Progress(Self::Waiting(waiting)),
                Ok(None) => Proceeded::Finished,
                Err(exn) => {
                    let (waiting, exn) = exn.recover();
                    match waiting {
                        Some(waiting) => Proceeded::Progress(Self::Waiting(waiting)),
                        None => return Err(exn),
                    }
                }
            },
        };
        Ok(proceeded)
    }
}

#[derive(Debug)]
pub enum Proceeded {
    SameAsBefore(ConversionJob),
    Progress(ConversionJob),
    Finished,
}

#[derive(Debug)]
pub struct ConversionJobWaiting {
    image_path: PathBuf,
    details: ConversionJobDetails,
    tool_use: ToolUse,
}

impl ConversionJobWaiting {
    fn start_conversion(self) -> Result<ConversionJobRunning, Exn<ErrorMessage>> {
        let ConversionJobWaiting {
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
            ToolUse::Backup(_) => match spawn::convert_with_magick(input, output) {
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

        Ok(ConversionJobRunning {
            child,
            image_path,
            details,
            tool_use,
        })
    }
}

#[derive(Debug)]
pub struct ConversionJobRunning {
    child: spawn::ManagedChild,
    image_path: PathBuf,
    details: ConversionJobDetails,
    tool_use: ToolUse,
}

impl ConversionJobRunning {
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

#[derive(Debug)]
pub struct ConversionJobCompleted(ConversionJobRunning);

impl ConversionJobCompleted {
    /// wait on child process and delete original image file
    fn complete(
        self,
    ) -> Result<Option<ConversionJobWaiting>, Exn<ErrorMessage, Option<ConversionJobWaiting>>> {
        let Self(ConversionJobRunning {
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
            ConversionJobDetails::OneStep { .. } | ConversionJobDetails::Finish { .. } => None,
            ConversionJobDetails::TwoStep { over: from, to, .. } => {
                let waiting = ConversionJobWaiting {
                    image_path: image_path.with_extension(from.ext()),
                    details: ConversionJobDetails::Finish { from, to },
                    tool_use: ToolUse::Best,
                };
                Some(waiting)
            }
        };
        Ok(after)
    }

    fn try_to_recover(
        exn: Exn<ErrorMessage>,
        image_path: PathBuf,
        details: ConversionJobDetails,
        tool_use: ToolUse,
    ) -> Result<Exn<ErrorMessage, ConversionJobWaiting>, Exn<ErrorMessage>> {
        if let ToolUse::Backup(last_exn) = tool_use {
            let msg = format!("Give up trying to convert {image_path:?}");
            let err = ErrorMessage::new(msg);
            return Err(Exn::raise_all(err, [last_exn, exn]));
        }

        let details = match details {
            ConversionJobDetails::OneStep { from, to }
            | ConversionJobDetails::TwoStep { from, to, .. } => ConversionJobDetails::TwoStep {
                from,
                over: ImageFormat::Png,
                to,
            },
            ConversionJobDetails::Finish { .. } => {
                let msg = "image from a previous pass could not be formatted further, \
                                something is gravely wrong";
                return Err(exn.raise(ErrorMessage::new(msg)));
            }
        };

        let msg = format!("Could not complete the conversion for {image_path:?}, try to recover");

        let waiting = ConversionJobWaiting {
            image_path,
            details,
            tool_use: ToolUse::Backup(exn),
        };

        Ok(Exn::with_recovery(ErrorMessage::new(msg), waiting))
    }
}

#[derive(Debug)]
pub enum ConversionJobDetails {
    OneStep {
        from: ImageFormat,
        to: ImageFormat,
    },
    TwoStep {
        from: ImageFormat,
        over: ImageFormat,
        to: ImageFormat,
    },
    Finish {
        from: ImageFormat,
        to: ImageFormat,
    },
}

impl ConversionJobDetails {
    pub fn new(
        image_path: &Path,
        current: ImageFormat,
        config: Configuration,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        use ImageFormat::*;

        let err = || {
            let current = current.ext();
            let target = config.target.ext();
            ErrorMessage::new(format!(
                "Failed to create the conversion job from {current:?} to {target:?} for {image_path:?}",
            ))
        };

        let Configuration { target, forced, .. } = config;

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

    fn perform_always(&self) -> bool {
        let tuple = match self {
            ConversionJobDetails::OneStep { from, to, .. } => (*from, *to),
            ConversionJobDetails::TwoStep { from, to, .. } => (*from, *to),
            ConversionJobDetails::Finish { .. } => return true,
        };

        use ImageFormat::*;
        match tuple {
            (Jpeg | Png, _) => true,
            (_, Jpeg | Png) => true,
            (_, _) => false,
        }
    }

    fn next_step(&self) -> (ImageFormat, ImageFormat) {
        match *self {
            ConversionJobDetails::OneStep { from, to } => (from, to),
            ConversionJobDetails::TwoStep { from, over, .. } => (from, over),
            ConversionJobDetails::Finish { from, to } => (from, to),
        }
    }

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

#[derive(Debug)]
enum ToolUse {
    Best,
    Backup(Exn<ErrorMessage>),
}

impl ToolUse {
    fn get_exn(self) -> Option<Exn<ErrorMessage>> {
        match self {
            ToolUse::Best => None,
            ToolUse::Backup(exn) => Some(exn),
        }
    }
}

pub struct ConversionJobs {
    job_queue: VecDeque<ConversionJob>,
    jobs_in_progress: Vec<Option<ConversionJob>>,
}

impl ConversionJobs {
    pub fn new(job_queue: VecDeque<ConversionJob>, concurrency: usize) -> Self {
        let jobs_in_progress = Vec::from_iter((0..concurrency).map(|_| None));
        Self {
            job_queue,
            jobs_in_progress,
        }
    }

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

    fn jobs_pending(&self) -> bool {
        self.jobs_in_progress.iter().any(|job| job.is_some())
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
pub enum ImageFormat {
    #[default]
    Jpeg,
    Png,
    Avif,
    Jxl,
    Webp,
}

impl ImageFormat {
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
        let out = ConversionJobDetails::jxl_is_compressed_jpeg(&compressed_path).unwrap();
        assert!(out);
    }

    #[test]
    fn test_check_for_encoded_jxl() {
        let encoded_path = PathBuf::from("test_data/encoded.jxl");
        assert!(encoded_path.exists());
        let out = ConversionJobDetails::jxl_is_compressed_jpeg(&encoded_path).unwrap();
        assert!(!out);
    }
}
