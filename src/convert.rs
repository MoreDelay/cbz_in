pub mod archive;
pub mod collections;
pub mod dir;
pub mod image;

use std::path::Path;

use exn::Exn;
use indicatif::{MultiProgress, ProgressBar};
use tracing::{error, info, warn};

use crate::error::ErrorMessage;

pub use collections::{ArchiveJobs, RecursiveDirJobs};
pub use image::ImageFormat;

#[derive(Debug, Clone, Copy)]
pub struct Configuration {
    pub target: ImageFormat,
    pub n_workers: usize,
    pub forced: bool,
}

pub trait Job {
    fn path(&self) -> &Path;

    fn run(self, bars: &ProgressBar) -> Result<(), Exn<ErrorMessage>>;
}

pub trait JobCollection: IntoIterator<Item = Self::Job> + Sized {
    type Job: Job;

    fn jobs(&self) -> usize;

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
                let err = ErrorMessage::new(format!("Failed to convert {n} job"));
                Err(Exn::raise_all(err, errors))
            }
        }
    }

    fn run_single(job: Self::Job, bars: &Bars) -> Result<(), Exn<ErrorMessage>> {
        info!("Converting {:?}", job.path());
        let print_res = bars.multi.println(format!("Converting {:?}", job.path()));
        if let Err(e) = print_res {
            warn!("Failed to write new message to stdout: {e:?}");
        }

        let run_res = job.run(&bars.images);
        bars.jobs.inc(1);
        match &run_res {
            Ok(_) => info!("Done"),
            Err(e) => error!("{e}"),
        }
        run_res
    }
}

pub struct Bars {
    multi: MultiProgress,
    jobs: ProgressBar,
    images: ProgressBar,
}

impl Bars {
    pub fn new(collection_type: CollectionType) -> Self {
        let multi = indicatif::MultiProgress::new();
        let jobs = match collection_type {
            CollectionType::Archives => multi.add(Self::create_progress_bar("Archives")),
            CollectionType::Directories => multi.add(Self::create_progress_bar("Directories")),
        };
        let images = multi.add(Self::create_progress_bar("Images"));

        jobs.enable_responsive_tick(std::time::Duration::from_millis(250));
        images.enable_responsive_tick(std::time::Duration::from_millis(250));

        Self {
            multi,
            jobs,
            images,
        }
    }

    pub fn jobs(&self) -> &ProgressBar {
        &self.jobs
    }

    pub fn images(&self) -> &ProgressBar {
        &self.images
    }

    fn create_progress_bar(msg: &'static str) -> indicatif::ProgressBar {
        const MSG_SPACE: usize = 9;

        assert!(msg.len() < MSG_SPACE);

        let style = indicatif::ProgressStyle::with_template(
            "[{elapsed_precise}] {msg:>9}: {wide_bar} {pos:>5}/{len:5}",
        )
        .unwrap();

        indicatif::ProgressBar::new(0)
            .with_style(style)
            .with_message(msg)
    }
}

pub enum CollectionType {
    Archives,
    Directories,
}
