//! Contains everything related to information gathering.

use std::io::BufRead as _;
use std::path::PathBuf;

use exn::{Exn, ResultExt as _};
use tracing::debug;
use walkdir::WalkDir;

use crate::convert::archive::ArchivePath;
use crate::convert::dir::Directory;
pub use crate::convert::image::ImageFormat;
use crate::error::ErrorMessage;
use crate::spawn::{self, ManagedChild};

/// Collection of all images found in an archive.
///
/// The image paths stored here are relative to the archive root.
pub struct ArchiveImages {
    /// The archive for which we store information.
    pub(super) archive: ArchivePath,
    /// All images found.
    pub(super) images: Vec<ImageInfo>,
}

impl ArchiveImages {
    /// Find all images in an archive.
    pub fn new(archive: ArchivePath) -> Result<Self, Exn<ErrorMessage>> {
        let err = || {
            let archive = archive.display();
            ErrorMessage::new(format!("Could not list files within archive \"{archive}\""))
        };

        debug!("Checking \"{}\"", archive.display());

        let images = spawn::list_archive_files(&archive)
            .and_then(ManagedChild::wait_with_output)
            .or_raise(err)?
            .stdout
            .lines()
            .filter_map(|line| {
                let line = match line {
                    Ok(line) => line,
                    Err(e) => return Some(Err(e)),
                };
                let path = line.strip_prefix("Path = ").map(PathBuf::from)?;
                Some(Ok(ImageInfo::new(path)?))
            })
            .collect::<Result<_, _>>()
            .or_raise(err)?;
        Ok(Self { archive, images })
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
    pub fn search_recursive(root: Directory) -> Result<Self, Exn<ErrorMessage>> {
        let err = || {
            let root = root.display();
            ErrorMessage::new(format!("Could not list files within directory \"{root}\""))
        };

        debug!("Checking \"{}\"", root.display());

        let images = WalkDir::new(&root)
            .same_file_system(true)
            .into_iter()
            .filter_map(|entry| {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => return Some(Err(e)),
                };
                let path = entry
                    .path()
                    .strip_prefix(&root)
                    .expect("all walked files have the root as prefix")
                    .to_path_buf();
                Some(Ok(ImageInfo::new(path)?))
            })
            .collect::<Result<_, _>>()
            .or_raise(err)?;
        Ok(Self { root, images })
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
