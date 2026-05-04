//! Contains the high level control flow items to execute the program.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::ops::Deref as _;
use std::path::PathBuf;

use exn::{ErrorExt as _, Exn, ResultExt as _};
use tracing::info;

use crate::convert::archive::{ArchiveJob, ArchivePath};
use crate::convert::collection::{ImageCollection, JobCollection};
use crate::convert::dir::{Directory, DirectoryJob};
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirectoryImages};
use crate::convert::{ConversionConfig, FilesystemRoot, ImagesJob};
use crate::error::{CompactReport, Msg};
use crate::stats::Stats;
use crate::{ConversionTarget, stdout};

/// All collections found in the locations specified by the user.
pub enum FoundImages {
    /// We looked for images in archives.
    Arc(ImageCollection<ArchiveImages>),
    /// We looked for images in directories.
    Dir(ImageCollection<DirectoryImages>),
}

impl FoundImages {
    /// Look for images as specified by the root type.
    pub fn search(
        paths: impl Iterator<Item = PathBuf>,
        root: FilesystemRoot,
    ) -> Result<Option<Self>, Exn<Msg<Self>>> {
        let err = || Msg::new("failed to find images");

        match root {
            FilesystemRoot::Archive => {
                let arcs = paths
                    .into_iter()
                    .map(|path| DirOrArchive::check(path)?.archive_iter())
                    .collect::<Result<Vec<_>, Exn<_>>>()
                    .or_raise(err)?
                    .into_iter()
                    .flatten();

                Ok(ImageCollection::in_archives(arcs)
                    .or_raise(err)?
                    .map(Self::Arc))
            }
            FilesystemRoot::Directory => {
                let dirs = paths
                    .into_iter()
                    .map(|path| {
                        let dir = Directory::new(path)?.map_err(|path| {
                            let msg = format!("Path is not a directory: \"{}\"", path.display());
                            Msg::new(msg).raise()
                        })?;
                        Ok(dir)
                    })
                    .collect::<Result<Vec<_>, Exn<_>>>()
                    .or_raise(err)?
                    .into_iter();

                Ok(ImageCollection::in_directories(dirs)
                    .or_raise(err)?
                    .map(Self::Dir))
            }
        }
    }

    /// Filter out all images such that only those remain that are specified in the filter.
    pub fn filter(self, filter: &HashSet<ImageFormat>) -> Option<Self> {
        match self {
            Self::Arc(images) => images.filter(filter).map(Self::Arc),
            Self::Dir(images) => images.filter(filter).map(Self::Dir),
        }
    }

    /// Print the number of images found per image format to stdout.
    pub fn print_stats(&self, verbose: bool) {
        match self {
            Self::Arc(images) => images.print_stats(verbose),
            Self::Dir(images) => images.print_stats(verbose),
        }
    }

    /// Run conversion on the found images.
    pub fn convert(
        self,
        target: ConversionTarget,
        n_workers: NonZeroUsize,
        no_log: bool,
    ) -> Result<(), Exn<Msg<Self>>> {
        let err = || Msg::new("Converting images");

        let config = ConversionConfig { target, n_workers };

        let Some(run) = RunConversion::new(self, config).or_raise(err)? else {
            stdout("Nothing to do");
            return Ok(());
        };

        run.test().or_raise(err)?;
        run.run(no_log).or_raise(err)
    }
}

/// A path that points either to a directory or to an archive.
enum DirOrArchive {
    /// The path points to an archive
    Arc(ArchivePath),
    /// The path points to a directory
    Dir(Directory),
}

impl DirOrArchive {
    /// Verify the path points either to a directory or an archive.
    pub fn check(path: PathBuf) -> Result<Self, Exn<Msg<Self>>> {
        let err = || Msg::new("Checking if path is a directory or an archive");

        let (path, dir_exn) = match Directory::new(path).or_raise(err)? {
            Ok(root) => return Ok(Self::Dir(root)),
            Err(path) => (path, Msg::new("Not a directory").raise()),
        };

        let (path, archive_exn) = match ArchivePath::new(path) {
            Ok(archive) => return Ok(Self::Arc(archive)),
            Err(exn) => exn.recover(),
        };

        let path = path.display();
        let msg = format!("Neither an archive nor a directory: \"{path}\"");
        let exn = Exn::raise_all(Msg::new(msg), [dir_exn, archive_exn]);
        Err(exn)
    }

    /// Convert this to a iterator over all applicable archives.
    ///
    /// When this is an archive directly, gives back just that one archive. When it is a directory,
    /// looks for any direct child files that are archives, and iterates over those.
    pub fn archive_iter(self) -> Result<impl Iterator<Item = ArchivePath>, Exn<Msg<Self>>> {
        match self {
            Self::Dir(dir) => {
                let flattened = Self::flatten_dir(&dir, true)?;
                Ok(either::Left(flattened.into_iter()))
            }
            Self::Arc(file) => Ok(either::Right(std::iter::once(file))),
        }
    }

    /// Find all child entries of this directory and collect those that are archives.
    fn flatten_dir(dir: &Directory, verbose: bool) -> Result<Vec<ArchivePath>, Exn<Msg<Self>>> {
        let err = || Msg::new("finding archives in directory");

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
    Arc(JobCollection<ArchiveJob>),
    /// We run conversion on directories.
    Dir(JobCollection<DirectoryJob>),
}

impl RunConversion {
    /// Prepare conversion jobs.
    fn new(found: FoundImages, config: ConversionConfig) -> Result<Option<Self>, Exn<Msg<Self>>> {
        let err = || Msg::new("Preparing conversion job");

        let out = match found {
            FoundImages::Arc(items) => JobCollection::new(items, config)
                .or_raise(err)?
                .map(Self::Arc),
            FoundImages::Dir(items) => JobCollection::new(items, config)
                .or_raise(err)?
                .map(Self::Dir),
        };

        Ok(out)
    }

    /// Run the conversion for real.
    fn run(self, no_log: bool) -> Result<(), Exn<Msg<Self>>> {
        let err = || Msg::new("Running conversion job");

        match self {
            Self::Arc(coll) => coll.run(no_log).or_raise(err)?,
            Self::Dir(coll) => coll.run(no_log).or_raise(err)?,
        }
        Ok(())
    }

    /// Get the root type stored in this struct.
    const fn root(&self) -> FilesystemRoot {
        match self {
            Self::Arc(_) => FilesystemRoot::Archive,
            Self::Dir(_) => FilesystemRoot::Directory,
        }
    }

    /// Check if we can run this job, and print out statistics.
    fn test(&self) -> Result<(), Exn<Msg<Self>>> {
        let err = || Msg::new("Doing dry run");

        self.check_tools().or_raise(err)?;
        self.print_conversion_header();

        let root = self.root().singular().to_ascii_lowercase();

        let paths: &mut dyn Iterator<Item = _> = match self {
            Self::Arc(jobs) => &mut jobs.iter().map(|j| j.path().deref()),
            Self::Dir(jobs) => &mut jobs.iter().map(|j| j.path().deref()),
        };

        for path in paths {
            info!("Got files to convert for {root} \"{}\"", path.display());
        }

        Ok(())
    }

    /// Check if all tools needed for this job are actually available.
    fn check_tools(&self) -> Result<(), Exn<Msg<Self>>> {
        let err = || Msg::new("Checking tool availability");

        let iter: &mut dyn Iterator<Item = _> = match self {
            Self::Arc(jobs) => &mut jobs.iter().flat_map(ImagesJob::iter),
            Self::Dir(jobs) => &mut jobs.iter().flat_map(ImagesJob::iter),
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
            .collect::<Result<Vec<_>, _>>()
            .or_raise(err)?;

        if !missing_tools.is_empty() {
            let mut missing_tools = missing_tools;
            missing_tools.sort_unstable();
            let tools = missing_tools.join(", ");
            let msg = format!("Missing tools: {tools}");
            let exn = Msg::new(msg).raise();
            return Err(exn);
        }
        Ok(())
    }

    /// Print out statistics on how many images will get converted by this job.
    fn print_conversion_header(&self) {
        let n_colls = match self {
            Self::Arc(jobs) => jobs.len(),
            Self::Dir(jobs) => jobs.len(),
        };

        let n_images: usize = match self {
            Self::Arc(jobs) => jobs.iter().map(ImagesJob::count).sum(),
            Self::Dir(jobs) => jobs.iter().map(ImagesJob::count).sum(),
        };

        let roots = self.root().plural().to_ascii_lowercase();

        stdout(format!(
            "Found {n_colls} {roots}, with a total of {n_images} images to convert"
        ));
    }
}
