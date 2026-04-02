//! Contains everything related to information gathering.

use std::collections::HashSet;
use std::io::BufRead as _;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use exn::{Exn, ResultExt as _};
use tracing::debug;
use walkdir::WalkDir;

use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
pub use crate::convert::image::ImageFormat;
use crate::error::ErrorMessage;
use crate::spawn::{self, ManagedChild};

/// Abstraction for collection of images that may be converted later.
pub trait ImageCollection: Sized {
    /// The filesystem type where images were collected.
    type Path: Deref<Target = Path>;

    /// Filter out all images that do not have the target image format
    fn filter(self, filter: &HashSet<ImageFormat>) -> Result<Self, Self::Path>;

    /// Provide metadata about all images stored in this collection.
    fn infos(&self) -> impl Iterator<Item = &ImageInfo>;

    /// Filter out all images that do not have the target image format
    fn path(&self) -> &Self::Path;
}

/// Collection of all images found in an archive.
///
/// The image paths stored here are relative to the archive root.
pub struct ArchiveImages {
    /// The archive for which we store information.
    pub archive: ArchivePath,
    /// All images found.
    pub images: Vec<ImageInfo>,
}

impl ArchiveImages {
    /// Find all images in an archive.
    pub fn new(archive: ArchivePath) -> Result<Result<Self, ArchivePath>, Exn<ErrorMessage>> {
        let err = || {
            let archive = archive.display();
            ErrorMessage::new(format!("Listing files within archive \"{archive}\""))
        };

        let images = spawn::list_archive_files(&archive)
            .and_then(ManagedChild::wait_with_output)
            .or_raise(err)?
            .stdout
            .lines()
            .map(|line| {
                let info = line?
                    .strip_prefix("Path = ")
                    .map(PathBuf::from)
                    .and_then(ImageInfo::new);
                Ok(info)
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, std::io::Error>>()
            .or_raise(err)?;

        if images.is_empty() {
            return Ok(Err(archive));
        }
        Ok(Ok(Self { archive, images }))
    }
}

impl ImageCollection for ArchiveImages {
    type Path = ArchivePath;

    fn filter(self, filter: &HashSet<ImageFormat>) -> Result<Self, Self::Path> {
        let images = self
            .images
            .into_iter()
            .filter(|info| filter.contains(&info.format()))
            .collect::<Vec<_>>();
        if images.is_empty() {
            return Err(self.archive);
        }
        Ok(Self { images, ..self })
    }

    fn infos(&self) -> impl Iterator<Item = &ImageInfo> {
        self.images.iter()
    }

    fn path(&self) -> &Self::Path {
        &self.archive
    }
}

impl IntoIterator for ArchiveImages {
    type Item = ImageInfo;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.images.into_iter()
    }
}

/// Collection of all images found in an archive.
///
/// The image paths stored here are relative to the root.
pub struct DirImages {
    /// The directory for which we store information.
    pub(super) root: Directory,
    /// All images found.
    pub(super) images: Vec<ImageInfo>,
}

impl DirImages {
    /// Find all images in a directory.
    pub fn search_recursive(root: Directory) -> Result<Result<Self, Directory>, Exn<ErrorMessage>> {
        let err = || {
            let root = root.display();
            ErrorMessage::new(format!("Listing files within directory \"{root}\""))
        };

        debug!("Checking \"{}\"", root.display());

        let images: Vec<ImageInfo> = WalkDir::new(&root)
            .same_file_system(true)
            .into_iter()
            .map(|entry| {
                let path = entry?
                    .path()
                    .strip_prefix(&root)
                    .expect("all walked files have the root as prefix")
                    .to_path_buf();
                Ok(ImageInfo::new(path))
            })
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, std::io::Error>>()
            .or_raise(err)?;

        if images.is_empty() {
            return Ok(Err(root));
        }
        Ok(Ok(Self { root, images }))
    }
}

impl ImageCollection for DirImages {
    type Path = Directory;

    fn filter(self, filter: &HashSet<ImageFormat>) -> Result<Self, Self::Path> {
        let images = self
            .images
            .into_iter()
            .filter(|info| filter.contains(&info.format()))
            .collect::<Vec<_>>();
        if images.is_empty() {
            return Err(self.root);
        }
        Ok(Self { images, ..self })
    }

    fn infos(&self) -> impl Iterator<Item = &ImageInfo> {
        self.images.iter()
    }

    fn path(&self) -> &Self::Path {
        &self.root
    }
}

impl IntoIterator for DirImages {
    type Item = ImageInfo;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.images.into_iter()
    }
}

/// Information about an image.
///
/// The file path stored here is relative. How to interpret the relative path depends on the
/// context on how we found the image in the first place, i.e. as part of an archive or a
/// directory.
pub struct ImageInfo {
    /// The relative path to the image.
    pub(super) path: PathBuf,
    /// The file type of the image.
    pub(super) format: ImageFormat,
}

impl ImageInfo {
    /// Get image information based on its filename.
    fn new(path: PathBuf) -> Option<Self> {
        let ext = path.extension()?.to_string_lossy().to_lowercase();
        let format = ext.parse().ok()?;
        Some(Self { path, format })
    }

    /// Get the image format.
    pub const fn format(&self) -> ImageFormat {
        self.format
    }
}
