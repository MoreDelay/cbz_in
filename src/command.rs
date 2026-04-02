//! Contains the high level control flow items to execute the program.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use exn::{ErrorExt as _, Exn, ResultExt as _};
use tracing::{debug, info};

use crate::convert::archive::ArchivePath;
use crate::convert::collection::{
    ArchiveJobCollection,
    DirectoryJobCollection,
    JobCollection as _,
};
use crate::convert::dir::Directory;
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirImages, ImageCollection};
use crate::convert::{ConversionConfig, Job};
use crate::error::{CompactReport, ErrorMessage};
use crate::stats::Stats;
use crate::{ConversionTarget, stdout};

/// A path that points either to a directory or to an archive.
pub enum DirOrArchive {
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

/// All collections found in the locations specified by the user.
pub enum FoundCollections {
    /// We looked for images in archives.
    Archives(Vec<ArchiveImages>),
    /// We looked for images in directories.
    Directories(Vec<DirImages>),
}

impl FoundCollections {
    /// Look for images in archives.
    pub fn on_archives(paths: impl Iterator<Item = PathBuf>) -> Result<Self, Exn<ErrorMessage>> {
        let found = paths
            .into_iter()
            .map(|path| DirOrArchive::check(path)?.archive_iter())
            .collect::<Result<Vec<_>, Exn<_>>>()?
            .into_iter()
            .flatten()
            .map(|path| {
                let images = ArchiveImages::new(path)?
                    .inspect_err(|path| {
                        debug!("Archive has no images: \"{}\"", path.display());
                    })
                    .ok();
                Ok(images)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, Exn<_>>>()?;
        Ok(Self::Archives(found))
    }

    /// Look for images in directories.
    pub fn on_dirs(paths: impl Iterator<Item = PathBuf>) -> Result<Self, Exn<ErrorMessage>> {
        let found = paths
            .into_iter()
            .map(|path| -> Result<Option<DirImages>, Exn<ErrorMessage>> {
                let dir = Directory::new(path)?.map_err(|path| {
                    let path = path.display();
                    let msg = format!("Path is not a directory: \"{path}\"");
                    ErrorMessage::new(msg).raise()
                })?;

                let images = DirImages::search_recursive(dir)?
                    .inspect_err(|dir| {
                        debug!("Directory has no images: \"{}\"", dir.display());
                    })
                    .ok();
                Ok(images)
            })
            .collect::<Result<Vec<_>, Exn<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(Self::Directories(found))
    }

    /// Filter out all images such that only those remain that are specified in the filter.
    pub fn filter(self, filter: &HashSet<ImageFormat>) -> Option<Self> {
        fn filter_item<T>(item: T, filter: &HashSet<ImageFormat>) -> Option<T>
        where
            T: ImageCollection,
        {
            item.filter(filter)
                .inspect_err(|path| {
                    debug!(
                        "Archive has no images left after filtering: \"{}\"",
                        path.display()
                    );
                })
                .ok()
        }

        match self {
            Self::Archives(items) => {
                let vec = items
                    .into_iter()
                    .filter_map(|item| filter_item(item, filter))
                    .collect::<Vec<_>>();
                Some(vec).filter(|v| !v.is_empty()).map(Self::Archives)
            }
            Self::Directories(items) => {
                let vec = items
                    .into_iter()
                    .filter_map(|item| filter_item(item, filter))
                    .collect::<Vec<_>>();
                Some(vec).filter(|v| !v.is_empty()).map(Self::Directories)
            }
        }
    }

    /// Print the number of images found per image format to stdout.
    pub fn print_stats(&self, verbose: bool) {
        let mut all_stats = Stats::new();

        match self {
            Self::Archives(items) => {
                let count = items.len();
                stdout(format!("Searched {count} archives:"));

                for item in items {
                    let stats = Stats::compute(item.infos());
                    all_stats.combine(&stats);

                    if verbose {
                        let header = format!("\"{}\":", item.path().display());
                        stdout(header);
                        stats.print_per_format();
                        stdout("---");
                        stats.print_total();
                        stdout(""); // new line
                    }
                }
            }
            Self::Directories(items) => {
                let count = items.len();
                stdout(format!("Searched {count} directories:"));

                for item in items {
                    let stats = Stats::compute(item.infos());
                    all_stats.combine(&stats);

                    if verbose {
                        let header = format!("\"{}\":", item.path().display());
                        stdout(header);
                        print_stats_per_format(&stats);
                        stdout(""); // new line
                    }
                }
            }
        }

        stdout("");
        print_stats_per_format(&all_stats);
        stdout("---");
        print_stats_total(&all_stats);
    }

    /// Run conversion on the found images.
    pub fn convert(
        self,
        target: ConversionTarget,
        n_workers: NonZeroUsize,
    ) -> Result<(), Exn<ErrorMessage>> {
        let config = ConversionConfig { target, n_workers };

        let Some(run) = RunConversion::new(self, config)? else {
            stdout("Nothing to do");
            return Ok(());
        };

        run.test()?;
        run.run()
    }
}

/// Print out statistics per image format.
pub fn print_stats_per_format(stats: &Stats) {
    let mut counts = Vec::from_iter(stats.inner.clone());
    counts.sort_unstable_by_key(|(f, _)| f.ext());
    for (format, count) in &counts {
        let format = format.ext();
        stdout(format!("{format}: {count}"));
    }
}

/// Print out the total number of images found.
pub fn print_stats_total(stats: &Stats) {
    let total: usize = stats.inner.values().sum();
    stdout(format!("total: {total}"));
}

/// Helper object to run conversion
enum RunConversion {
    /// We run conversion on archives.
    Archives(ArchiveJobCollection),
    /// We run conversion on directories.
    Directories(DirectoryJobCollection),
}

impl RunConversion {
    /// Prepare conversion jobs.
    fn new(
        found: FoundCollections,
        config: ConversionConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let out = match found {
            FoundCollections::Archives(items) => {
                ArchiveJobCollection::new(items, config)?.map(Self::Archives)
            }
            FoundCollections::Directories(items) => {
                DirectoryJobCollection::new(items, config)?.map(Self::Directories)
            }
        };

        Ok(out)
    }

    /// Run the conversion for real.
    fn run(self) -> Result<(), Exn<ErrorMessage>> {
        match self {
            Self::Archives(coll) => coll.run()?,
            Self::Directories(coll) => coll.run()?,
        }
        Ok(())
    }

    /// Check if we can run this job, and print out statistics.
    fn test(&self) -> Result<(), Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Doing dry run");

        self.check_tools().or_raise(err)?;
        self.print_statistics();

        let paths: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.iter().map(Job::path),
            Self::Directories(jobs) => &mut jobs.iter().map(Job::path),
        };

        for path in paths {
            info!("Got files to convert for \"{}\"", path.display());
        }

        Ok(())
    }

    /// Check if all tools needed for this job are actually available.
    fn check_tools(&self) -> Result<(), Exn<ErrorMessage>> {
        let iter: &mut dyn Iterator<Item = _> = match self {
            Self::Archives(jobs) => &mut jobs.iter().flat_map(Job::iter),
            Self::Directories(jobs) => &mut jobs.iter().flat_map(Job::iter),
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
            let exn = ErrorMessage::new(msg).raise();
            return Err(exn);
        }
        Ok(())
    }

    /// Print out statistics on how many images would get converted by this job.
    fn print_statistics(&self) {
        let collections = match self {
            Self::Archives(jobs) => jobs.len(),
            Self::Directories(jobs) => jobs.len(),
        };

        let images: usize = match self {
            Self::Archives(jobs) => jobs.iter().map(Job::count).sum(),
            Self::Directories(jobs) => jobs.iter().map(Job::count).sum(),
        };

        let coll_type = match self {
            Self::Archives(_) => "archives",
            Self::Directories(_) => "directories",
        };

        stdout(format!(
            "Found {collections} {coll_type}, with a total of {images} images to convert"
        ));
    }
}
