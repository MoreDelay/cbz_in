//! Contains items related to gathering statistics about found images.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

use exn::{Exn, ResultExt as _};
use tracing::info;

use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
use crate::convert::image::ImageFormat;
use crate::convert::search::{ArchiveImages, DirImages, ImageInfo};
use crate::error::{CompactReport, ErrorMessage};
use crate::{stdout, verbose};

/// Our job is to print statistics about the images we find.
pub struct StatsJob {
    /// The images we found, for which will we print statistics.
    images: Images,
    /// What statistics to print and how.
    config: StatsConfig,
}

impl StatsJob {
    /// Prepare to print statistics for archives.
    pub fn on_archives(
        paths: VecDeque<PathBuf>,
        config: StatsConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all archives");

        stdout("Counting images in archives...");

        let job = Images::within_archives(paths, &config)
            .or_raise(err)?
            .map(|images| Self { images, config });
        Ok(job)
    }

    /// Prepare to print statistics for directories.
    pub fn on_directories(
        paths: VecDeque<PathBuf>,
        config: StatsConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        stdout("Counting images in directories...");

        let job = Images::within_directories(paths, &config)
            .or_raise(err)?
            .map(|images| Self { images, config });
        Ok(job)
    }

    /// Run this job.
    pub fn run(self) {
        match &self.images {
            Images::Archives(images) => {
                let count = images.len();
                stdout(format!("Searched {count} archives:"));
            }
            Images::Directories(images) => {
                let count = images.len();
                stdout(format!("Searched {count} directories:"));
            }
        }

        if self.config.verbose {
            match self.images {
                Images::Archives(images) => images.print_per_archive(),
                Images::Directories(images) => images.print_per_dir(),
            }
        } else {
            let images: &mut dyn Iterator<Item = _> = match self.images {
                Images::Archives(images) => &mut images.into_info(),
                Images::Directories(images) => &mut images.into_info(),
            };
            Self::print_image_stats(images);
        }
    }

    /// Get an iterator to print out non-verbose statistics.
    fn print_image_stats(images: &mut dyn Iterator<Item = ImageInfo>) {
        let stats = Stats::compute(images);
        stats.print_per_format();
        stdout("---");
        stats.print_total();
    }
}

/// The images we found during the search pass.
enum Images {
    /// We searched within archives.
    Archives(PerArchiveImages),
    /// We searched within directories.
    Directories(PerDirImages),
}

impl Images {
    /// Collect images found within archives.
    fn within_archives(
        paths: VecDeque<PathBuf>,
        config: &StatsConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all archives");

        let images = PerArchiveImages::collect(paths, config)
            .or_raise(err)?
            .map(Self::Archives);
        Ok(images)
    }

    /// Collect images found within directories.
    fn within_directories(
        paths: VecDeque<PathBuf>,
        config: &StatsConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        let images = PerDirImages::collect(paths, config)
            .or_raise(err)?
            .map(Self::Directories);
        Ok(images)
    }
}

/// All images found per archive.
struct PerArchiveImages(Vec<ArchiveImages>);

impl PerArchiveImages {
    /// Collect images found in archives.
    fn collect(
        paths: VecDeque<PathBuf>,
        config: &StatsConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let collect_single = |path| {
            let (path, dir_exn) = match Directory::new(path)? {
                Ok(root) => return Self::for_archives_in_dir(&root, config),
                Err(exn) => exn.recover(),
            };

            let (path, archive_exn) = match ArchivePath::new(path) {
                Ok(archive) => {
                    let vec = (Self::for_single_archive(archive, config)?)
                        .map_or_else(Vec::new, |a| vec![a]);
                    return Ok(vec);
                }
                Err(exn) => exn.recover(),
            };

            let path = path.display();
            let msg = format!("Neither an archive nor a directory: \"{path}\"");
            let exn = Exn::raise_all(ErrorMessage::new(msg), [dir_exn, archive_exn]);
            Err(exn)
        };

        let err = || ErrorMessage::new("Failed to collect all archives");

        let images = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .collect();
        Ok(Some(Self(images)))
    }

    /// Get the number of archives stored in this container.
    const fn len(&self) -> usize {
        self.0.len()
    }

    /// Convert this container into an iterator of all [`ImageInfo`]'s.
    fn into_info(self) -> impl Iterator<Item = ImageInfo> {
        self.0.into_iter().flatten()
    }

    /// Create an [`ArchiveImages`] for a single archive.
    fn for_single_archive(
        archive: ArchivePath,
        config: &StatsConfig,
    ) -> Result<Option<ArchiveImages>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to search images in a single archive");

        let msg = format!("Checking archive \"{}\"", archive.display());
        verbose(config.verbose, msg);

        let images = match ArchiveImages::new(archive).or_raise(err)? {
            Ok(images) => images,
            Err(path) => {
                let msg = format!("    No images in \"{}\"", path.display());
                verbose(config.verbose, msg);
                return Ok(None);
            }
        };
        match config.filter {
            Some(target) => Ok(images.filter(target)),
            None => Ok(Some(images)),
        }
    }

    /// Create an [`ArchiveImages`] for all archives in a directory.
    fn for_archives_in_dir(
        root: &Directory,
        config: &StatsConfig,
    ) -> Result<Vec<ArchiveImages>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to search images in all archives in a directory");

        info!("Checking archives directory \"{}\"", root.display());
        root.read_dir()
            .or_raise(err)?
            .map(|dir_entry| {
                let path = dir_entry.or_raise(err)?.path();
                let archive = match ArchivePath::new(path) {
                    Ok(archive) => archive,
                    Err(exn) => {
                        let (path, exn) = exn.recover();
                        let path = path.display();
                        let report = CompactReport::new(exn);
                        verbose(config.verbose, format!("Skipping \"{path}\": {report}"));
                        return Ok(None);
                    }
                };
                Self::for_single_archive(archive, config)
            })
            .filter_map(Result::transpose)
            .collect()
    }

    /// Get an iterator to print out verbose archive statistics.
    fn print_per_archive(self) {
        let mut all_stats = Stats::new();

        for archive in self.0 {
            stdout(format!("\"{}\":", archive.path().display()));
            let images = &mut archive.into_iter();
            let stats = Stats::compute(images);
            all_stats.combine(&stats);

            stats.print_per_format();
            stdout("");
        }

        stdout("");
        all_stats.print_per_format();
        stdout("---");
        all_stats.print_total();
    }
}

/// All images found per directory.
struct PerDirImages(Vec<DirImages>);

impl PerDirImages {
    /// Prepare to print statistics for directories.
    fn collect(
        paths: VecDeque<PathBuf>,
        config: &StatsConfig,
    ) -> Result<Option<Self>, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new("Failed to collect all directories");

        let collect_single = |path| {
            let root = Directory::new(path)?.map_err(Exn::discard_recovery)?;
            let images = DirImages::search_recursive(root)?;
            match config.filter {
                Some(target) => Ok(images.and_then(|images| images.filter(target))),
                None => Ok(images),
            }
        };

        let images = paths
            .into_iter()
            .map(collect_single)
            .collect::<Result<Vec<_>, Exn<_>>>()
            .or_raise(err)?
            .into_iter()
            .flatten()
            .collect();
        Ok(Some(Self(images)))
    }

    /// Get the number of archives stored in this container.
    const fn len(&self) -> usize {
        self.0.len()
    }

    /// Convert this container into an iterator of all [`ImageInfo`]'s.
    fn into_info(self) -> impl Iterator<Item = ImageInfo> {
        self.0.into_iter().flatten()
    }

    /// Get an iterator to print out verbose directory statistics.
    fn print_per_dir(self) {
        let mut all_stats = Stats::new();

        for dir in self.0 {
            stdout(format!("\"{}\":", dir.path().display()));
            let images = &mut dir.into_iter();
            let stats = Stats::compute(images);
            all_stats.combine(&stats);

            stats.print_per_format();
            stdout("");
        }

        stdout("");
        all_stats.print_per_format();
        stdout("---");
        all_stats.print_total();
    }
}

/// Aggregated statistics.
struct Stats {
    /// Maps an image format to the number of occurences
    inner: HashMap<ImageFormat, usize>,
}

impl Stats {
    /// Create a new, empty statistics object.
    fn new() -> Self {
        let inner = HashMap::new();
        Self { inner }
    }

    /// Count the occurences of each image type in the iterator.
    fn compute(images: &mut dyn Iterator<Item = ImageInfo>) -> Self {
        let inner = images.fold(HashMap::new(), |mut counts, info| {
            counts
                .entry(info.format())
                .and_modify(|v| *v += 1)
                .or_insert(1);
            counts
        });
        Self { inner }
    }

    /// Combine two statistics into one
    fn combine(&mut self, other: &Self) {
        for (&format, &count) in &other.inner {
            self.inner
                .entry(format)
                .and_modify(|v| *v += count)
                .or_insert(count);
        }
    }

    /// Print out statistics per image format.
    fn print_per_format(&self) {
        let mut counts = Vec::from_iter(self.inner.clone());
        counts.sort_unstable_by_key(|(f, _)| f.ext());
        for (format, count) in &counts {
            let format = format.ext();
            stdout(format!("{format}: {count}"));
        }
    }

    /// Print out the total number of images found.
    fn print_total(&self) {
        let total: usize = self.inner.values().sum();
        stdout(format!("total: {total}"));
    }
}

/// Configuration for what statistics to collect.
pub struct StatsConfig {
    /// Filter for a specific image format.
    pub filter: Option<ImageFormat>,
    /// Print out more detailed information.
    pub verbose: bool,
}
