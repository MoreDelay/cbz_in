pub mod archive;
pub mod collections;
pub mod dir;
pub mod image;

use std::{num::NonZeroUsize, path::Path};

use exn::Exn;
use indicatif::{MultiProgress, ProgressBar};
use tracing::{error, info, warn};

use crate::error::ErrorMessage;

pub use collections::{ArchiveJobs, RecursiveDirJobs};
pub use image::ImageFormat;

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

    /// Run this job
    fn run(self, bars: &ProgressBar) -> Result<(), Exn<ErrorMessage>>;
}

/// A trait for a collection of jobs that internally run jobs themselves.
pub trait JobCollection: IntoIterator<Item = Self::Single> + Sized {
    type Single: Job;

    fn jobs(&self) -> usize;

    /// Run all internal jobs, showing the progress in [Bars].
    fn run(self, bars: &Bars) -> Result<(), Exn<ErrorMessage>> {
        bars.jobs.reset();
        bars.jobs.set_length(self.jobs() as u64);

        let errors = self
            .into_iter()
            .map(|job| Self::run_single(job, bars))
            .filter_map(|res| res.err())
            .collect::<Vec<_>>();
        match errors.is_empty() {
            true => Ok(()),
            false => {
                let n = errors.len();
                let err = ErrorMessage::new(format!("Failed to complete {n} job"));
                Err(Exn::raise_all(err, errors))
            }
        }
    }

    /// Run one of the jobs that is part of the collection.
    fn run_single(job: Self::Single, bars: &Bars) -> Result<(), Exn<ErrorMessage>> {
        info!("Converting {:?}", job.path());
        bars.println(format!("Converting {:?}", job.path()));

        let run_res = job.run(&bars.images);
        bars.jobs.inc(1);
        match &run_res {
            Ok(_) => info!("Done"),
            Err(e) => {
                bars.println(format!("ERROR: {e}"));
                error!("{e}");
            }
        }
        run_res
    }
}

/// A set of progress bars used to indicate the progress of the conversion.
pub struct Bars {
    multi: MultiProgress,
    jobs: ProgressBar,
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

    pub fn println(&self, msg: impl AsRef<str>) {
        let msg = msg.as_ref();
        if let Err(e) = self.multi.println(msg) {
            warn!("Failed to write new message to stdout: {e:?}\nOriginal message: {msg}");
        }
    }

    /// Create a new progress bar with hard-coded style.
    fn create_progress_bar(title: &'static str) -> indicatif::ProgressBar {
        const MSG_SPACE: usize = 9;

        assert!(title.len() < MSG_SPACE, "title does not fit: {title}");

        let style = indicatif::ProgressStyle::with_template(
            "[{elapsed_precise}] {msg:>9}: {wide_bar} {pos:>5}/{len:5}",
        )
        .unwrap();

        indicatif::ProgressBar::new(0)
            .with_style(style)
            .with_message(title)
            .with_finish(indicatif::ProgressFinish::AndLeave)
    }
}

/// All different titles that can be given to the [Bars::jobs] progress bar.
#[derive(Debug, Clone, Copy)]
pub enum JobsBarTitle {
    Archives,
    Directories,
}

impl JobsBarTitle {
    /// Get the title for the bar as string.
    fn name(self) -> &'static str {
        match self {
            JobsBarTitle::Archives => "Archives",
            JobsBarTitle::Directories => "Dirs",
        }
    }
}
