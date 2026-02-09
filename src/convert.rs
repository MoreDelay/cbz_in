//! Contains everything related to performing conversions.

pub mod archive;
pub mod collections;
pub mod dir;
pub mod image;
pub mod search;

use std::num::NonZeroUsize;
use std::path::Path;

use exn::Exn;
use indicatif::{MultiProgress, ProgressBar};
use tracing::{error, info, warn};

use crate::convert::image::{ConversionJob, ImageFormat};
use crate::error::{ErrorMessage, got_interrupted};

/// General configuration for a run of any conversion job.
#[derive(Debug, Clone, Copy)]
pub struct Configuration {
    /// The target image format to which all files get converted.
    pub target: ImageFormat,
    /// How many processes to run at most at any given time.
    pub n_workers: NonZeroUsize,
    /// Force conversion of all image files, or just conversion of Jpeg and Png.
    pub forced: bool,
}

/// A trait for jobs that can be run.
pub trait Job {
    /// Get a path for this job, that best describes its scope of operation.
    fn path(&self) -> &Path;

    /// Get an iterator over all image conversion jobs for inspection.
    fn iter(&self) -> impl Iterator<Item = &ConversionJob>;

    /// Run this job.
    fn run(self, bar: &ProgressBar) -> Result<(), Exn<ErrorMessage>>;
}

/// A trait for a collection of jobs that internally run jobs themselves.
pub trait JobCollection: IntoIterator<Item = Self::Single> + Sized {
    /// The job type which of which this is a collection of.
    type Single: Job;

    /// Get an iterator over all image conversion jobs for inspection.
    fn jobs(&self) -> impl Iterator<Item = &Self::Single>;

    /// Run all internal jobs, showing the progress in [Bars].
    fn run(self, bars: &Bars) -> Result<(), Exn<ErrorMessage>> {
        bars.jobs.reset();
        bars.jobs.set_length(self.jobs().count() as u64);

        let errors = self
            .into_iter()
            .map(|job| Self::run_single(job, bars))
            .filter_map(|res| {
                let err = res.err()?;

                // Stop all further jobs if we got interrupted
                if got_interrupted(&err) {
                    Some(Err(err))
                } else {
                    Some(Ok(err))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !errors.is_empty() {
            let n = errors.len();
            let err = ErrorMessage::new(format!("Failed to complete {n} job(s)"));
            return Err(Exn::raise_all(err, errors));
        }
        Ok(())
    }

    /// Run one of the jobs that is part of the collection.
    fn run_single(job: Self::Single, bars: &Bars) -> Result<(), Exn<ErrorMessage>> {
        let path = job.path().display();
        info!("Converting \"{path}\"");
        bars.println(format!("Converting \"{path}\""));

        let run_res = job.run(&bars.images);
        match &run_res {
            Ok(()) => {
                bars.jobs.inc(1);
                info!("Done");
            }
            Err(err) => {
                bars.println(format!("ERROR: {err}"));
                error!("{err}");
            }
        }
        run_res
    }
}

/// A set of progress bars used to indicate the progress of the conversion.
pub struct Bars {
    /// The multi bar containing all other bars in this struct.
    multi: MultiProgress,
    /// The progress bar for overarching jobs.
    jobs: ProgressBar,
    /// The progress bar for individual image conversions.
    images: ProgressBar,
}

impl Bars {
    /// Create a new set of progress bars that will immediately be displayed on the terminal.
    pub fn new(collection_type: JobsBarTitle) -> Self {
        let multi = indicatif::MultiProgress::new();
        let jobs = multi.add(Self::create_progress_bar(collection_type.name()));
        let images = multi.add(Self::create_progress_bar("Images"));

        jobs.enable_responsive_tick(std::time::Duration::from_millis(250));
        images.enable_responsive_tick(std::time::Duration::from_millis(250));

        Self {
            multi,
            jobs,
            images,
        }
    }

    /// Print a message above our progress bars.
    pub fn println(&self, msg: impl AsRef<str>) {
        let msg = msg.as_ref();
        if let Err(e) = self.multi.println(msg) {
            warn!("Failed to write a message to console: {e:?}\nOriginal message: {msg}");
        }
    }

    /// Finish progress on bars for the "happy path".
    ///
    /// Dropping [Bars] without calling this method indicates we exited irregularly.
    pub fn finish(self) {
        self.jobs.finish();
        self.images.finish();
    }

    /// Create a new progress bar with hard-coded style.
    fn create_progress_bar(title: &'static str) -> indicatif::ProgressBar {
        const MSG_SPACE: usize = 9;

        assert!(title.len() < MSG_SPACE, "title does not fit: {title}");

        #[expect(clippy::literal_string_with_formatting_args)]
        let style = indicatif::ProgressStyle::with_template(
            "[{elapsed_precise}] {msg:>9}: {wide_bar} {pos:>5}/{len:5}",
        )
        .expect("valid template");

        indicatif::ProgressBar::new(0)
            .with_style(style)
            .with_message(title)
            .with_finish(indicatif::ProgressFinish::Abandon)
    }
}

/// All different titles that can be given to the [`Bars::jobs`] progress bar.
#[derive(Debug, Clone, Copy)]
pub enum JobsBarTitle {
    /// Indicate we work on archives.
    Archives,
    /// Indicate we work on directories.
    Directories,
}

impl JobsBarTitle {
    /// Get the title for the bar as string.
    const fn name(self) -> &'static str {
        match self {
            Self::Archives => "Archives",
            Self::Directories => "Dirs",
        }
    }
}
