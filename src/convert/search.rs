//! Contains everything related to information gathering.

use std::collections::HashSet;
use std::io::BufRead as _;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use exn::{Exn, ResultExt as _};
use walkdir::WalkDir;

use crate::convert::FilesystemRoot;
use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
use crate::convert::image::ImageFormat;
use crate::error::Msg;
use crate::spawn::{self, ManagedChild};

/// Abstraction for collection of images that may be converted later.
pub trait Images: Sized {
    /// The filesystem type where images were collected.
    type Path: Deref<Target = Path>;

    /// Get the filesystem entry that is the root of the found images.
    fn fs_root() -> FilesystemRoot;

    /// Find all images in the specified root.
    fn search(root: Self::Path) -> Result<Result<Self, Self::Path>, Exn<Msg<Self>>>;

    /// Filter out all images that do not have the target image format.
    fn filter(self, filter: &HashSet<ImageFormat>) -> Result<Self, Self::Path>;

    /// Provide metadata about all images stored in this collection.
    fn infos(&self) -> impl Iterator<Item = &ImageInfo>;

    /// Filter out all images that do not have the target image format.
    fn path(&self) -> &Self::Path;
}

/// Collection of all images found in an archive.
///
/// The image paths stored here are relative to the archive root.
pub struct ArchiveImages {
    /// The archive for which we store information.
    pub root: ArchivePath,
    /// All images found.
    pub images: Vec<ImageInfo>,
}

impl Images for ArchiveImages {
    type Path = ArchivePath;

    fn fs_root() -> FilesystemRoot {
        FilesystemRoot::Archive
    }

    fn search(root: Self::Path) -> Result<Result<Self, Self::Path>, Exn<Msg<Self>>> {
        let err = || {
            let root = root.display();
            Msg::new(format!("Listing files within archive \"{root}\""))
        };

        let images = spawn::list_archive_files(&root)
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
            return Ok(Err(root));
        }
        Ok(Ok(Self { root, images }))
    }

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
pub struct DirectoryImages {
    /// The directory for which we store information.
    pub(super) root: Directory,
    /// All images found.
    pub(super) images: Vec<ImageInfo>,
}

impl Images for DirectoryImages {
    type Path = Directory;

    fn fs_root() -> FilesystemRoot {
        FilesystemRoot::Directory
    }

    fn search(root: Self::Path) -> Result<Result<Self, Self::Path>, Exn<Msg<Self>>> {
        let err = || {
            let root = root.display();
            Msg::new(format!("Listing files within directory \"{root}\""))
        };

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

impl IntoIterator for DirectoryImages {
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
    pub path: PathBuf,
    /// The file type of the image.
    pub format: ImageFormat,
}

impl ImageInfo {
    /// Get image information based on its filename.
    #[must_use]
    pub fn new(path: PathBuf) -> Option<Self> {
        let ext = path.extension()?.to_string_lossy().to_lowercase();
        let format = ext.parse().ok()?;
        Some(Self { path, format })
    }

    /// Get the image format.
    #[must_use]
    pub const fn format(&self) -> ImageFormat {
        self.format
    }
}
