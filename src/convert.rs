//! Contains everything related to performing conversions.

pub mod archive;
pub mod collection;
pub mod dir;
pub mod image;
pub mod search;

use std::num::NonZeroUsize;
use std::path::PathBuf;

use exn::{ErrorExt as _, Exn, ResultExt as _};
use indicatif::{MultiProgress, ProgressBar};
use tracing::warn;

use crate::ConversionTarget;
use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
use crate::convert::image::ConversionJob;
use crate::convert::search::Images;
use crate::error::{CompactReport, ErrorMessage, NothingToDo};

/// General configuration for a run of any conversion job.
#[derive(Debug, Clone, Copy)]
pub struct ConversionConfig {
    /// The target image format to which source files get converted.
    pub target: ConversionTarget,
    /// How many processes to run at most at any given time.
    pub n_workers: NonZeroUsize,
}

/// Type alias to reduce type clutter specifying the [`Job`]'s path type.
type JobPath<J> = <<J as Job>::Images as Images>::Path;

/// A trait for jobs that can be run.
pub trait Job: Sized {
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
    fn run(self, bar: &ProgressBar) -> Result<(), Exn<ErrorMessage>>;
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

/// A path that points either to a directory or to an archive.
enum DirOrArchive {
    /// The path points to a directory
    Directory(Directory),
    /// The path points to an archive
    Archive(ArchivePath),
}

impl DirOrArchive {
    /// Verify the path points either to a directory or an archive.
    pub fn check(path: PathBuf) -> Result<Self, Exn<ErrorMessage>> {
        let (path, dir_exn) = match Directory::new(path)? {
            Ok(root) => return Ok(Self::Directory(root)),
            Err(path) => (path, ErrorMessage::new("Not a directory").raise()),
        };

        let (path, archive_exn) = match ArchivePath::new(path) {
            Ok(archive) => return Ok(Self::Archive(archive)),
            Err(exn) => exn.recover(),
        };

        let path = path.display();
        let msg = format!("Neither an archive nor a directory: \"{path}\"");
        let exn = Exn::raise_all(ErrorMessage::new(msg), [dir_exn, archive_exn]);
        Err(exn)
    }

    /// Convert this to a iterator over all applicable archives.
    ///
    /// When this is an archive directly, gives back just that one archive. When it is a directory,
    /// looks for any direct child files that are archives, and iterates over those.
    pub fn archive_iter(self) -> Result<impl Iterator<Item = ArchivePath>, Exn<ErrorMessage>> {
        match self {
            Self::Directory(dir) => {
                let flattened = Self::flatten_dir(&dir, true)?;
                Ok(either::Left(flattened.into_iter()))
            }
            Self::Archive(file) => Ok(either::Right(std::iter::once(file))),
        }
    }

    /// Find all child entries of this directory and collect those those that are archives.
    fn flatten_dir(dir: &Directory, verbose: bool) -> Result<Vec<ArchivePath>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("finding archives in directory");

        dir.read_dir()
            .or_raise(err)?
            .map(|dir_entry| {
                let path = dir_entry.or_raise(err)?.path();
                match ArchivePath::new(path) {
                    Ok(archive) => Ok(Some(archive)),
                    Err(exn) => {
                        let (path, exn) = exn.recover();
                        let path = path.display();
                        let report = CompactReport::new(&exn);
                        crate::verbose(verbose, format!("Skipping \"{path}\": {report}"));
                        Ok(None)
                    }
                }
            })
            .filter_map(Result::transpose)
            .collect()
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
    const fn singular(self) -> &'static str {
        match self {
            Self::Archive => "Archive",
            Self::Directory => "Directory",
        }
    }

    /// Get the plural name for this root type.
    const fn plural(self) -> &'static str {
        match self {
            Self::Archive => "Archives",
            Self::Directory => "Directories",
        }
    }
}
