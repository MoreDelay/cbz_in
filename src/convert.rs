//! Contains everything related to performing conversions.

pub mod archive;
pub mod collection;
pub mod dir;
pub mod image;
pub mod search;

use std::num::NonZeroUsize;

use exn::Exn;
use indicatif::{MultiProgress, ProgressBar};
use tracing::warn;

use crate::ConversionTarget;
use crate::convert::image::ConversionJob;
use crate::convert::search::Images;
use crate::error::{ErrorMessage, NothingToDo};

/// General configuration for a run of any conversion job.
#[derive(Debug, Clone, Copy)]
pub struct ConversionConfig {
    /// The target image format to which source files get converted.
    pub target: ConversionTarget,
    /// How many processes to run at most at any given time.
    pub n_workers: NonZeroUsize,
}

/// Type alias to reduce type clutter specifying the [`Job`]'s path type.
type JobPath<J> = <<J as ImagesJob>::Images as Images>::Path;

/// A trait for jobs that converts some [`Images`].
pub trait ImagesJob: Sized {
    /// The image collection this job works on
    type Images: Images;

    /// Create a new job over a collection of images.
    fn new(
        images: Self::Images,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo<JobPath<Self>>>, Exn<ErrorMessage>>;

    /// Get a path for this job, that best describes its scope of operation.
    fn path(&self) -> &JobPath<Self>;

    /// Get an iterator over all image conversion jobs for inspection.
    fn iter(&self) -> impl Iterator<Item = &ConversionJob>;

    /// Get the number of increment steps required for the progress bar.
    fn count(&self) -> usize;

    /// Run this job.
    fn run(self, bar: Option<&ProgressBar>) -> Result<(), Exn<ErrorMessage>>;
}

/// A set of progress bars used to indicate the progress of the conversion.
pub struct Bars {
    /// The multi bar containing all other bars in this struct.
    pub multi: MultiProgress,
    /// The progress bar for overarching jobs.
    pub jobs: ProgressBar,
    /// The progress bar for individual image conversions.
    pub images: ProgressBar,
}

impl Bars {
    /// Create a new set of progress bars that will immediately be displayed on the terminal.
    pub fn new(name: FilesystemRoot) -> Self {
        let multi = indicatif::MultiProgress::new();
        let jobs = multi.add(Self::create_progress_bar(name.plural()));
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
    /// Dropping [`Bars`] without calling this method indicates we exited irregularly.
    pub fn finish(self) {
        self.jobs.finish();
        self.images.finish();
    }

    /// Create a new progress bar with hard-coded style.
    fn create_progress_bar(title: &'static str) -> indicatif::ProgressBar {
        #[expect(clippy::literal_string_with_formatting_args)]
        let style = indicatif::ProgressStyle::with_template(
            "[{elapsed_precise}] {msg}: {wide_bar} {pos:>5}/{len:5}",
        )
        .expect("valid template");

        indicatif::ProgressBar::new(0)
            .with_style(style)
            .with_message(title)
            .with_finish(indicatif::ProgressFinish::Abandon)
    }
}

/// The different filesystem roots where we find and convert images within.
#[derive(Debug, Clone, Copy)]
pub enum FilesystemRoot {
    /// On the filesystem, this is an archive.
    Archive,
    /// On the filesystem, this is a directory.
    Directory,
}

impl FilesystemRoot {
    /// Get the plural name for this root type.
    pub const fn singular(self) -> &'static str {
        match self {
            Self::Archive => "Archive",
            Self::Directory => "Directory",
        }
    }

    /// Get the plural name for this root type.
    pub const fn plural(self) -> &'static str {
        match self {
            Self::Archive => "Archives",
            Self::Directory => "Directories",
        }
    }
}
