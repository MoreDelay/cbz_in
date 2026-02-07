use std::io::BufRead;
use std::path::PathBuf;

use exn::{Exn, ResultExt};
use walkdir::WalkDir;

use crate::{
    convert::{archive::ArchivePath, dir::Directory},
    error::ErrorMessage,
    spawn,
};

use super::ImageFormat;

/// Collection of all images found in an archive.
///
/// The image paths stored here are relative to the archive root.
pub struct ArchiveImages {
    pub(super) archive: ArchivePath,
    pub(super) images: Vec<ImageInfo>,
}

impl ArchiveImages {
    /// Find all images in an archive.
    pub fn new(archive: ArchivePath) -> Result<Self, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new(format!("Could not list files within archive {archive:?}"));

        let images = spawn::list_archive_files(&archive)
            .and_then(|c| c.wait_with_output())
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

/// Collection of all images found in an archive.
///
/// The image paths stored here are relative to the root.
pub struct DirImages {
    pub(super) root: Directory,
    pub(super) images: Vec<ImageInfo>,
}

impl DirImages {
    /// Find all images in a directory..
    pub fn new(root: Directory) -> Result<Self, Exn<ErrorMessage>> {
        let err = || ErrorMessage::new(format!("Could not list files within directory {root:?}"));

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

/// Information about an image.
///
/// The file path stored here is relative. How to interpret the relative path depends on the
/// context on how we found the image in the first place, i.e. as part of an archive or a
/// directory.
pub struct ImageInfo {
    pub(super) path: PathBuf,
    pub(super) format: ImageFormat,
}

impl ImageInfo {
    /// Get image information based on its filename.
    fn new(path: PathBuf) -> Option<Self> {
        use ImageFormat::*;
        let ext = path.extension()?.to_string_lossy().to_lowercase();
        let path = match ext.as_str() {
            "jpg" => Some((path, Jpeg)),
            "jpeg" => Some((path, Jpeg)),
            "png" => Some((path, Png)),
            "avif" => Some((path, Avif)),
            "jxl" => Some((path, Jxl)),
            "webp" => Some((path, Webp)),
            _ => None,
        };
        path.map(|(path, format)| ImageInfo { path, format })
    }
}
