//! Tests that call the main function of the binary.

#![allow(clippy::tests_outside_test_module)]

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::str::FromStr as _;

use cbz_in::{ArchiveImages, ArchivePath, Directory, DirectoryImages, ImageFormat, Images};
use clap::Parser as _;
use tempfile::TempDir;

struct TestDir {
    root: TempDir,
}

impl TestDir {
    fn new(prefix: &str) -> anyhow::Result<Self> {
        let root = TempDir::with_prefix(prefix)?;
        Self::copy_recursively("tests/data", root.path())?;
        Ok(Self { root })
    }

    fn copy_recursively(from: impl AsRef<Path>, to: impl AsRef<Path>) -> anyhow::Result<()> {
        let from = from.as_ref();
        let to = to.as_ref();

        std::fs::create_dir_all(to)?;

        for entry in fs::read_dir(from)? {
            let entry = entry?;
            let from = &entry.path();
            let to = to.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                Self::copy_recursively(from, to)?;
            } else {
                fs::copy(from, to)?;
            }
        }
        Ok(())
    }
}

#[expect(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ConversionExpectation {
    target: ImageFormat,
    jpeg: bool,
    png: bool,
    avif: bool,
    jxl: bool,
    webp: bool,
}

impl ConversionExpectation {
    const fn target(target: ImageFormat) -> Self {
        Self {
            target,
            jpeg: false,
            png: false,
            avif: false,
            jxl: false,
            webp: false,
        }
    }

    const fn jpeg(mut self) -> Self {
        self.jpeg = true;
        self
    }

    const fn png(mut self) -> Self {
        self.png = true;
        self
    }

    const fn avif(mut self) -> Self {
        self.avif = true;
        self
    }

    const fn jxl(mut self) -> Self {
        self.jxl = true;
        self
    }

    const fn webp(mut self) -> Self {
        self.webp = true;
        self
    }

    const fn all(self) -> Self {
        self.jpeg().png().avif().jxl().webp()
    }

    fn any_wrong_format_in(&self, images: &impl Images) -> Option<ImageFormat> {
        for info in images.infos() {
            let (prev, cur) = if let Some(name) = info.path.file_stem()
                && let Some(name) = name.to_str()
                && let Ok(prev) = ImageFormat::from_str(name)
                && let Some(ext) = info.path.extension()
                && let Some(ext) = ext.to_str()
                && let Ok(cur) = ImageFormat::from_str(ext)
            {
                (prev, cur)
            } else {
                continue;
            };

            let expected = match prev {
                ImageFormat::Jpeg if self.jpeg => self.target,
                ImageFormat::Png if self.png => self.target,
                ImageFormat::Avif if self.avif => self.target,
                ImageFormat::Jxl if self.jxl => self.target,
                ImageFormat::Webp if self.webp => self.target,

                any @ (ImageFormat::Jpeg
                | ImageFormat::Png
                | ImageFormat::Avif
                | ImageFormat::Jxl
                | ImageFormat::Webp) => any,
            };

            if cur != expected {
                return Some(prev);
            }
        }
        None
    }
}

#[test]
fn convert_zip() {
    let test_dir = TestDir::new("convert_zip").expect("can create temp dirs");
    let zip = test_dir.root.path().join("zip.zip");

    let cmd = [
        OsStr::new("cbz_in"),
        OsStr::new("jpeg"),
        OsStr::new("--no-log"),
        zip.as_os_str(),
    ];
    let expectation = ConversionExpectation::target(ImageFormat::Jpeg)
        .jpeg()
        .png();

    let args = cbz_in::Args::try_parse_from(cmd).expect("correct command");
    cbz_in::entry_point(args).expect("converts without issues");

    let zip = test_dir.root.path().join("zip.jpeg.zip");
    assert!(zip.exists());
    let zip = ArchivePath::new(zip).expect("is zip");
    let images = ArchiveImages::search(zip)
        .expect("can read zip")
        .expect("has images");

    assert_eq!(expectation.any_wrong_format_in(&images), None);
}

#[test]
fn convert_cbz() {
    let test_dir = TestDir::new("convert_cbz").expect("can create temp dirs");
    let cbz = test_dir.root.path().join("cbz.cbz");

    let cmd = [
        OsStr::new("cbz_in"),
        OsStr::new("png"),
        OsStr::new("--no-log"),
        cbz.as_os_str(),
    ];
    let expectation = ConversionExpectation::target(ImageFormat::Png).jpeg().png();

    let args = cbz_in::Args::try_parse_from(cmd).expect("correct command");
    cbz_in::entry_point(args).expect("converts without issues");

    let cbz = test_dir.root.path().join("cbz.png.cbz");
    assert!(cbz.exists());
    let cbz = ArchivePath::new(cbz).expect("is zip");
    let images = ArchiveImages::search(cbz)
        .expect("can read cbz")
        .expect("has images");

    assert_eq!(expectation.any_wrong_format_in(&images), None);
}

#[test]
fn convert_dir() {
    let test_dir = TestDir::new("convert_dir").expect("can create temp dirs");
    let dir = test_dir.root.path().join("dir");

    let cmd = [
        OsStr::new("cbz_in"),
        OsStr::new("png"),
        OsStr::new("--from=all"),
        OsStr::new("--no-archive"),
        OsStr::new("--no-log"),
        dir.as_os_str(),
    ];
    let expectation = ConversionExpectation::target(ImageFormat::Png).all();

    let args = cbz_in::Args::try_parse_from(cmd).expect("correct command");
    cbz_in::entry_point(args).expect("converts without issues");

    let dir = test_dir.root.path().join("dir-png");
    assert!(dir.exists());
    let dir = Directory::new(dir)
        .expect("can read file system")
        .expect("is dir");
    let images = DirectoryImages::search(dir)
        .expect("can read zip")
        .expect("has images");

    assert_eq!(expectation.any_wrong_format_in(&images), None);
}
