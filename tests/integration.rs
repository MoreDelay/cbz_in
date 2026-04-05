//! Tests that call the main function of the binary.

#![allow(clippy::tests_outside_test_module)]

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::str::FromStr as _;

use cbz_in::{
    ArchiveImages,
    ArchivePath,
    ConversionSource,
    ConversionTarget,
    Directory,
    DirectoryImages,
    ImageFormat,
    Images,
};
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
#[derive(Debug, Clone, Copy)]
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

    fn any_wrong_format_in(self, images: &impl Images) -> Option<ImageFormat> {
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

#[derive(Debug, Clone, Copy)]
enum TestFile {
    Zip,
    Cbz,
    Dir,
}

enum OutImages {
    Arc(ArchiveImages),
    Dir(DirectoryImages),
}

impl OutImages {
    fn check_against(&self, expectation: ConversionExpectation) -> Option<ImageFormat> {
        match self {
            Self::Arc(images) => expectation.any_wrong_format_in(images),
            Self::Dir(images) => expectation.any_wrong_format_in(images),
        }
    }
}

fn run_test(test_file: TestFile, target: ConversionTarget, source: ConversionSource) -> TestDir {
    let test_dir = TestDir::new("test-cbz_in-").expect("can create temp dirs");
    let file = match test_file {
        TestFile::Zip => "zip.zip",
        TestFile::Cbz => "cbz.cbz",
        TestFile::Dir => "dir",
    };
    let file = test_dir.root.path().join(file);

    let source_arg = format!("--from={source}");
    // let log_path = test_dir.root.path().join("log.log");
    let mut cmd = vec![
        OsStr::new("cbz_in"),
        OsStr::new("--no-log"),
        // OsStr::new("--log-path"),
        // log_path.as_os_str(),
        // OsStr::new("--level=debug"),
        OsStr::new(&source_arg),
        OsStr::new(target.format().ext()),
    ];
    if target == (ConversionTarget::Jxl { lossy: true }) {
        cmd.push(OsStr::new("--lossy"));
    }
    if matches!(test_file, TestFile::Dir) {
        cmd.push(OsStr::new("--no-archive"));
    }
    cmd.push(file.as_os_str());
    println!("{cmd:#?}");

    let args = cbz_in::Args::try_parse_from(cmd)
        .map_err(|s| s.to_string())
        .expect("correct command");
    cbz_in::entry_point(args).expect("converts without issues");

    let expectation = ConversionExpectation::target(target.format());
    let expectation = match source {
        ConversionSource::All => expectation.all(),
        ConversionSource::Jpeg => expectation.jpeg(),
        ConversionSource::Png => expectation.png(),
        ConversionSource::Avif => expectation.avif(),
        ConversionSource::Jxl => expectation.jxl(),
        ConversionSource::Webp => expectation.webp(),
    };

    let out_file = match test_file {
        TestFile::Zip => format!("zip.{}.zip", target.format().ext()),
        TestFile::Cbz => format!("cbz.{}.cbz", target.format().ext()),
        TestFile::Dir => format!("dir-{}", target.format().ext()),
    };
    let out_file = test_dir.root.path().join(out_file);
    assert!(
        out_file.exists(),
        "This file should be produced by the program"
    );

    let images = match test_file {
        TestFile::Zip | TestFile::Cbz => {
            let out_file = ArchivePath::new(out_file).expect("is zip");
            let images = ArchiveImages::search(out_file)
                .expect("can read cbz")
                .expect("has images");
            OutImages::Arc(images)
        }
        TestFile::Dir => {
            let out_file = Directory::new(out_file)
                .expect("can read dir")
                .expect("is dir");
            let images = DirectoryImages::search(out_file)
                .expect("can read dir")
                .expect("has images");
            OutImages::Dir(images)
        }
    };

    assert_eq!(
        images.check_against(expectation),
        None,
        "If not none, then this image format is not converted as expected"
    );

    test_dir
}

#[test]
fn convert_zip() {
    let test_file = TestFile::Zip;
    let target = ConversionTarget::Jpeg;
    let source = ConversionSource::Png;
    run_test(test_file, target, source);
}

#[test]
fn convert_cbz() {
    let test_file = TestFile::Cbz;
    let target = ConversionTarget::Png;
    let source = ConversionSource::Jpeg;
    run_test(test_file, target, source);
}

#[test]
fn convert_dir() {
    let test_file = TestFile::Dir;
    let target = ConversionTarget::Png;
    let source = ConversionSource::All;
    run_test(test_file, target, source);
}

#[test]
fn two_step_webp() {
    let test_file = TestFile::Cbz;
    let target = ConversionTarget::Webp;
    let source = ConversionSource::All;
    run_test(test_file, target, source);
}

#[test]
fn two_step_avif() {
    let test_file = TestFile::Cbz;
    let target = ConversionTarget::Avif;
    let source = ConversionSource::All;
    run_test(test_file, target, source);
}

#[test]
fn jxl_lossy() {
    let test_file = TestFile::Dir;
    let target = ConversionTarget::Jxl { lossy: true };
    let source = ConversionSource::Jpeg;
    let test_dir = run_test(test_file, target, source);

    let out_dir = format!("dir-{}", target.format().ext());
    let out_file = "jpeg.jxl";
    let out_file = test_dir.root.path().join(out_dir).join(out_file);
    assert!(out_file.exists(), "Expect converted jxl to exist");

    let compressed = cbz_in::jxl_is_compressed_jpeg(&out_file).expect("can read metadata");
    assert!(!compressed);
}

#[test]
fn jxl_lossless() {
    let test_file = TestFile::Dir;
    let target = ConversionTarget::Jxl { lossy: false };
    let source = ConversionSource::Jpeg;
    let test_dir = run_test(test_file, target, source);

    let out_dir = format!("dir-{}", target.format().ext());
    let out_file = "jpeg.jxl";
    let out_file = test_dir.root.path().join(out_dir).join(out_file);
    assert!(out_file.exists(), "Expect converted jxl to exist");

    let compressed = cbz_in::jxl_is_compressed_jpeg(&out_file).expect("can read metadata");
    assert!(compressed);
}
