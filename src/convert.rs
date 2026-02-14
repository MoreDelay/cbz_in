//! Contains everything related to performing conversions.

pub mod archive;
pub mod collections;
pub mod dir;
pub mod image;
pub mod search;

use std::collections::{HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use exn::{Exn, ResultExt as _, bail};
use indicatif::{MultiProgress, ProgressBar};
use tracing::{info, warn};

use crate::convert::archive::ArchivePath;
use crate::convert::collections::{ArchiveJobs, RecursiveDirJobs};
use crate::convert::dir::Directory;
use crate::convert::image::{ConversionJob, ImageFormat};
use crate::error::{ErrorMessage, got_interrupted};
use crate::stdout;

/// General configuration for a run of any conversion job.
#[derive(Debug, Clone, Copy)]
pub struct ConversionConfig {
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
        bars.jobs.inc(1);
        match &run_res {
            Ok(()) => info!("Done"),
            Err(err) => bars.println(format!("ERROR: {err}")),
        }
        run_res
    }
}

/// Our job is to convert images in archives or directories.
pub enum ConvertJob {
    /// We work on archives.
    Archives(ArchiveJobs),
    /// We work on directories.
    Directories(RecursiveDirJobs),
}

impl ConvertJob {
    /// Create a [`ConvertJob::Archives`].
    pub fn on_archives(
        paths: VecDeque<PathBuf>,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path)? {
                Ok(root) => return Self::for_archives_in_dir(&root, config),
                Err(exn) => exn.recover(),
            };

            let (path, archive_exn) = match ArchivePath::new(path) {
                Ok(archive) => return Self::for_single_archive(archive, config),
                Err(exn) => exn.recover(),
            };

            let msg = format!("Neither an archive nor a directory: \"{}\"", path.display());
            let exn = Exn::raise_all(ErrorMessage::new(msg), [dir_exn, archive_exn]);
            Err(exn)
        };

        let err = || ErrorMessage::new("Failed to collect all archives");

        stdout("Looking for images to convert in archives...");

        let jobs = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .flatten();
        let jobs = ArchiveJobs::aggregate(jobs).map(Self::Archives);
        Ok(jobs)
    }

    /// Create [`ConvertJob::Directories`].
    pub fn on_directories(
        paths: VecDeque<PathBuf>,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        let collect_single = |path| {
            let root = Directory::new(path)?.map_err(Exn::discard_recovery)?;
            Self::for_images_within_dir(root, config)
        };

        stdout("Looking for images to convert in directories...");

        let jobs = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .flatten();
        let jobs = RecursiveDirJobs::aggregate(jobs).map(Self::Directories);
        Ok(jobs)
    }

    /// Run this job.
    pub fn run(self, dry_run: bool) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("failed to run conversion job");

        self.dry_run().or_raise(err)?;
        if dry_run {
            return Ok(());
        }

        let collection_type = match self {
            Self::Archives(_) => JobsBarTitle::Archives,
            Self::Directories(_) => JobsBarTitle::Directories,
        };
        let bars = Bars::new(collection_type);

        match self {
            Self::Archives(jobs) => jobs.run(&bars)?,
            Self::Directories(jobs) => jobs.run(&bars)?,
        }

        bars.finish();
        Ok(())
    }

    /// Check if we can run this job, and print out statistics.
    fn dry_run(&self) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Issue encountered during dry run");

        self.check_tools().or_raise(err)?;
        self.print_statistics();

        let paths: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.jobs().map(Job::path),
            Self::Directories(jobs) => &mut jobs.jobs().map(Job::path),
        };

        for path in paths {
            info!("Got files to convert for \"{}\"", path.display());
        }

        Ok(())
    }

    /// Check if all tools needed for this job are actually available.
    fn check_tools(&self) -> Result<(), Exn<ErrorMessage>> {
        let iter: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.jobs().flat_map(Job::iter),
            Self::Directories(jobs) => &mut jobs.jobs().flat_map(Job::iter),
        };
        let required_tools = iter
            .flat_map(|job| job.plan().required_tools())
            .collect::<HashSet<_>>();
        let missing_tools = required_tools
            .into_iter()
            .filter_map(|tool| match tool.available() {
                Ok(true) => None,
                Ok(false) => Some(Ok(tool.name())),
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !missing_tools.is_empty() {
            let mut missing_tools = missing_tools;
            missing_tools.sort_unstable();
            let tools = missing_tools.join(", ");
            let msg = format!("Missing tools: {tools}");
            bail!(ErrorMessage::new(msg))
        }
        Ok(())
    }

    /// Print out statistics on how many images would get converted by this job.
    fn print_statistics(&self) {
        let collections = match self {
            Self::Archives(jobs) => jobs.jobs().count(),
            Self::Directories(jobs) => jobs.jobs().count(),
        };

        let images = match self {
            Self::Archives(jobs) => &mut jobs.jobs().flat_map(Job::iter).count(),
            Self::Directories(jobs) => &mut jobs.jobs().flat_map(Job::iter).count(),
        };

        let coll_type = match self {
            Self::Archives(_) => "archives",
            Self::Directories(_) => "directories",
        };

        stdout(format!(
            "Found {collections} {coll_type}, with a total of {images} images to convert"
        ));
    }

    /// Create an [`ArchiveJobs`] for a single archive.
    fn for_single_archive(
        archive: ArchivePath,
        config: ConversionConfig,
    ) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to create conversion job on a single archive");

        info!("Checking archive \"{}\"", archive.display());
        ArchiveJobs::single(archive, config).or_raise(err)
    }

    /// Create an [`ArchiveJobs`] for all archives in a directory.
    fn for_archives_in_dir(
        root: &Directory,
        config: ConversionConfig,
    ) -> Result<Option<ArchiveJobs>, Exn<ErrorMessage>> {
        let err =
            || ErrorMessage::new("Failed to create conversion job for all archives in a directory");

        info!("Checking archives directory \"{}\"", root.display());
        ArchiveJobs::collect(root, config).or_raise(err)
    }

    /// Create a [`RecursiveDirJobs`] for a directory.
    fn for_images_within_dir(
        root: Directory,
        config: ConversionConfig,
    ) -> Result<Option<RecursiveDirJobs>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to create conversion job for a directory");

        info!("Checking root directory recursively \"{}\"", root.display());
        RecursiveDirJobs::single(root, config).or_raise(err)
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
