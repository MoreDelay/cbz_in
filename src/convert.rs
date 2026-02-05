use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use exn::{ErrorExt, Exn, OptionExt, ResultExt, bail};
use indicatif::{MultiProgress, ProgressBar};
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use tracing::{debug, error, info};
use walkdir::WalkDir;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::{ErrorMessage, spawn};

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
pub enum ImageFormat {
    #[default]
    Jpeg,
    Png,
    Avif,
    Jxl,
    Webp,
}

impl ImageFormat {
    fn ext(self) -> &'static str {
        use ImageFormat::*;

        match self {
            Jpeg => "jpeg",
            Png => "png",
            Avif => "avif",
            Jxl => "jxl",
            Webp => "webp",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchivePath(PathBuf);

impl std::ops::Deref for ArchivePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ArchivePath {
    const ARCHIVE_EXTENSIONS: [&str; 2] = ["zip", "cbz"];

    pub fn new(archive_path: PathBuf) -> Result<Self, Exn<ErrorMessage, PathBuf>> {
        let correct_extension = archive_path.extension().is_some_and(|ext| {
            Self::ARCHIVE_EXTENSIONS
                .iter()
                .any(|valid_ext| ext.eq_ignore_ascii_case(valid_ext))
        });
        if !correct_extension {
            let msg = format!("Archive has an unsupported extension: {archive_path:?}");
            debug!("{msg}");
            let exn = Exn::with_recovery(ErrorMessage(msg), archive_path);
            return Err(exn);
        }

        if !archive_path.is_file() {
            let msg = format!("Archive does not exist: {archive_path:?}");
            debug!("{msg}");
            let exn = Exn::with_recovery(ErrorMessage(msg), archive_path);
            return Err(exn);
        }

        Ok(ArchivePath(archive_path))
    }

    fn into_inner(self) -> PathBuf {
        self.0
    }
}

#[derive(Debug)]
enum ConversionJobDetails {
    OneStep {
        from: ImageFormat,
        to: ImageFormat,
    },
    TwoStep {
        from: ImageFormat,
        over: ImageFormat,
        to: ImageFormat,
    },
    Finish {
        from: ImageFormat,
        to: ImageFormat,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct ConversionConfig {
    pub target: ImageFormat,
    pub n_workers: usize,
    pub forced: bool,
}

impl ConversionJobDetails {
    fn new(
        image_path: &Path,
        current: ImageFormat,
        config: ConversionConfig,
    ) -> exn::Result<Option<Self>, ErrorMessage> {
        use ImageFormat::*;

        let err = || {
            let msg = format!(
                "Failed to create the conversion job from {:?} to {:?} for {image_path:?}",
                current.ext(),
                config.target.ext()
            );
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let ConversionConfig { target, forced, .. } = config;

        let out = match (current, target) {
            (a, b) if a == b => return Ok(None),
            (Avif, Jxl | Webp) => Self::TwoStep {
                from: current,
                over: Png,
                to: target,
            },
            (Jxl, Avif | Webp) => match Self::jxl_is_compressed_jpeg(image_path).or_raise(err)? {
                true => Self::TwoStep {
                    from: current,
                    over: Jpeg,
                    to: target,
                },
                false => Self::TwoStep {
                    from: current,
                    over: Png,
                    to: target,
                },
            },
            (Webp, Jpeg | Avif | Jxl) => Self::TwoStep {
                from: current,
                over: Png,
                to: target,
            },
            (_, _) => Self::OneStep {
                from: current,
                to: target,
            },
        };
        let perform = forced || out.perform_always();
        Ok(perform.then_some(out))
    }

    fn perform_always(&self) -> bool {
        let tuple = match self {
            ConversionJobDetails::OneStep { from, to, .. } => (*from, *to),
            ConversionJobDetails::TwoStep { from, to, .. } => (*from, *to),
            ConversionJobDetails::Finish { .. } => return true,
        };

        use ImageFormat::*;
        match tuple {
            (Jpeg | Png, _) => true,
            (_, Jpeg | Png) => true,
            (_, _) => false,
        }
    }

    fn next_step(&self) -> (ImageFormat, ImageFormat) {
        match *self {
            ConversionJobDetails::OneStep { from, to } => (from, to),
            ConversionJobDetails::TwoStep { from, over, .. } => (from, over),
            ConversionJobDetails::Finish { from, to } => (from, to),
        }
    }

    fn jxl_is_compressed_jpeg(image_path: &Path) -> exn::Result<bool, ErrorMessage> {
        let err = || {
            let msg = format!("Could not query jxl file {image_path:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let has_box = spawn::run_jxlinfo(image_path)
            .and_then(|c| c.wait_with_output())
            .or_raise(err)?
            .stdout
            .lines()
            .any(|line| line.unwrap().starts_with("box: type: \"jbrd\""));
        Ok(has_box)
    }
}

#[derive(Debug)]
enum ToolUse {
    Best,
    Backup(Exn<ErrorMessage>),
}

impl ToolUse {
    fn get_exn(self) -> Option<Exn<ErrorMessage>> {
        match self {
            ToolUse::Best => None,
            ToolUse::Backup(exn) => Some(exn),
        }
    }
}

#[derive(Debug)]
struct ConversionJobWaiting {
    image_path: PathBuf,
    details: ConversionJobDetails,
    tool_use: ToolUse,
}
#[derive(Debug)]
struct ConversionJobRunning {
    child: spawn::ManagedChild,
    image_path: PathBuf,
    details: ConversionJobDetails,
    tool_use: ToolUse,
}
#[derive(Debug)]
struct ConversionJobCompleted(ConversionJobRunning);
#[derive(Debug)]
enum ConversionJob {
    Waiting(ConversionJobWaiting),
    Running(ConversionJobRunning),
    Completed(ConversionJobCompleted),
}

#[derive(Debug, Error)]
pub enum NothingToDo {
    #[error("No files to convert in '{0}'")]
    NoFilesToConvert(PathBuf),
    #[error("Already converted '{0}'")]
    AlreadyDone(PathBuf),
}

#[derive(Debug)]
enum Proceeded {
    SameAsBefore(ConversionJob),
    Progress(ConversionJob),
    Finished,
}

impl ConversionJobWaiting {
    fn start_conversion(self) -> exn::Result<ConversionJobRunning, ErrorMessage> {
        let ConversionJobWaiting {
            image_path,
            details,
            tool_use,
        } = self;
        let err = || {
            let msg = format!("Failed image conversion for {image_path:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };

        debug!("start conversion for {image_path:?}: {details:?}");

        let (current, target) = details.next_step();
        let input_path = &image_path;
        let output_path = &image_path.with_extension(target.ext());

        use ImageFormat::*;
        let child = match tool_use {
            ToolUse::Best => match (current, target) {
                (Jpeg, Png) => spawn::convert_jpeg_to_png(input_path, output_path).or_raise(err)?,
                (Png, Jpeg) => spawn::convert_png_to_jpeg(input_path, output_path).or_raise(err)?,
                (Jpeg | Png, Avif) => spawn::encode_avif(input_path, output_path).or_raise(err)?,
                (Jpeg | Png, Jxl) => spawn::encode_jxl(input_path, output_path).or_raise(err)?,
                (Jpeg | Png, Webp) => spawn::encode_webp(input_path, output_path).or_raise(err)?,
                (Avif, Jpeg) => {
                    spawn::decode_avif_to_jpeg(input_path, output_path).or_raise(err)?
                }
                (Avif, Png) => spawn::decode_avif_to_png(input_path, output_path).or_raise(err)?,
                (Jxl, Jpeg) => spawn::decode_jxl_to_jpeg(input_path, output_path).or_raise(err)?,
                (Jxl, Png) => spawn::decode_jxl_to_png(input_path, output_path).or_raise(err)?,
                (Webp, Png) => spawn::decode_webp(input_path, output_path).or_raise(err)?,
                (_, _) => unreachable!(),
            },
            ToolUse::Backup(_) => match spawn::convert_with_magick(input_path, output_path) {
                Ok(child) => child,
                Err(exn) => {
                    let last_exn = tool_use
                        .get_exn()
                        .expect("checked by match that we can get exn");
                    let exn = Exn::raise_all(err(), [last_exn, exn]);
                    return Err(exn);
                }
            },
        };

        Ok(ConversionJobRunning {
            child,
            image_path,
            details,
            tool_use,
        })
    }
}

impl ConversionJobRunning {
    fn child_done(&mut self) -> exn::Result<bool, ErrorMessage> {
        let err = || {
            let msg = format!(
                "Could not check if a process finished for {:?}",
                self.image_path
            );
            debug!("{msg}");
            ErrorMessage(msg)
        };
        self.child.try_wait().or_raise(err)
    }
}

impl ConversionJobCompleted {
    /// wait on child process and delete original image file
    fn complete(
        self,
    ) -> Result<Option<ConversionJobWaiting>, Exn<ErrorMessage, Option<ConversionJobWaiting>>> {
        let Self(ConversionJobRunning {
            child,
            image_path,
            details,
            tool_use,
        }) = self;

        debug!("completed conversion for {image_path:?}: {details:?}");

        let err = || {
            let msg = format!("Could not complete the conversion for {image_path:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };

        if let Err(exn) = child.wait() {
            match tool_use {
                ToolUse::Best => {
                    let details = match details {
                        ConversionJobDetails::OneStep { from, to }
                        | ConversionJobDetails::TwoStep { from, to, .. } => {
                            ConversionJobDetails::TwoStep {
                                from,
                                over: ImageFormat::Png,
                                to,
                            }
                        }
                        ConversionJobDetails::Finish { .. } => {
                            let msg = "image from a previous pass could not be formatted further, something is gravely wrong".to_string();
                            debug!("{msg}");
                            let exn = exn.raise_with_recovery(ErrorMessage(msg), None);
                            return Err(exn);
                        }
                    };

                    let msg = format!(
                        "Could not complete the conversion for {image_path:?}, try to recover"
                    );
                    debug!("{msg}");

                    let waiting = ConversionJobWaiting {
                        image_path,
                        details,
                        tool_use: ToolUse::Backup(exn),
                    };

                    let exn = Exn::with_recovery(ErrorMessage(msg), Some(waiting));
                    return Err(exn);
                }
                ToolUse::Backup(last_exn) => {
                    let exn = Exn::raise_all_with_recovery(err(), [last_exn, exn], None);
                    return Err(exn);
                }
            }
        }
        // at this point we have successfully converted the image and prepare the next conversion
        drop(tool_use);

        fs::remove_file(&image_path).or_raise_with_recovery(err, None)?;

        let after = match details {
            ConversionJobDetails::OneStep { .. } | ConversionJobDetails::Finish { .. } => None,
            ConversionJobDetails::TwoStep { over: from, to, .. } => {
                let waiting = ConversionJobWaiting {
                    image_path: image_path.with_extension(from.ext()),
                    details: ConversionJobDetails::Finish { from, to },
                    tool_use: ToolUse::Best,
                };
                Some(waiting)
            }
        };
        Ok(after)
    }
}

impl ConversionJob {
    fn new(image_path: PathBuf, details: ConversionJobDetails) -> Self {
        let waiting = ConversionJobWaiting {
            image_path,
            details,
            tool_use: ToolUse::Best,
        };
        Self::Waiting(waiting)
    }

    fn proceed(self) -> exn::Result<Proceeded, ErrorMessage> {
        let proceeded = match self {
            ConversionJob::Waiting(waiting) => {
                let running = waiting.start_conversion()?;
                Proceeded::Progress(Self::Running(running))
            }
            ConversionJob::Running(mut running) => match running.child_done()? {
                false => Proceeded::SameAsBefore(Self::Running(running)),
                true => {
                    let completed = ConversionJobCompleted(running);
                    Proceeded::Progress(Self::Completed(completed))
                }
            },
            ConversionJob::Completed(completed) => match completed.complete() {
                Ok(Some(waiting)) => Proceeded::Progress(Self::Waiting(waiting)),
                Ok(None) => Proceeded::Finished,
                Err(exn) => {
                    let (waiting, exn) = exn.recover();
                    match waiting {
                        Some(waiting) => Proceeded::Progress(Self::Waiting(waiting)),
                        None => bail!(exn),
                    }
                }
            },
        };
        Ok(proceeded)
    }
}

struct ExtractionJob {
    archive_path: ArchivePath,
}

impl ExtractionJob {
    fn run(self) -> exn::Result<TempDirGuard, ErrorMessage> {
        let err = || {
            let msg = format!("Failed to extract the archive {:?}", self.archive_path);
            debug!("{msg}");
            ErrorMessage(msg)
        };

        assert!(self.archive_path.is_file());

        let extract_dir = ArchiveJob::get_conversion_root_dir(&self.archive_path);
        assert!(!extract_dir.exists());

        let guard = TempDirGuard::new(extract_dir.to_path_buf());
        fs::create_dir_all(&extract_dir)
            .or_raise(|| {
                let msg = format!("Could not create the target directory at {extract_dir:?}");
                debug!("{msg}");
                ErrorMessage(msg)
            })
            .or_raise(err)?;
        spawn::extract_zip(&self.archive_path, &extract_dir)
            .and_then(|c| c.wait())
            .or_raise(err)?;
        Ok(guard)
    }
}

/// Deletes the temporary directory when dropped
///
/// To keep the directory, use `guard.keep()`
struct TempDirGuard {
    temp_root: Option<PathBuf>,
}

impl TempDirGuard {
    fn new(temp_root: PathBuf) -> Self {
        let temp_root = Some(temp_root);
        Self { temp_root }
    }

    fn keep(mut self) {
        self.temp_root.take();
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let Some(root) = self.temp_root.take() else {
            return;
        };
        debug!("drop temporary directory {root:?}");
        if root.exists()
            && let Err(e) = fs::remove_dir_all(&root)
        {
            error!("error on deleting directory {root:?}: {e}");
        }
    }
}

struct CompressionJob {
    root: PathBuf,
    target: ImageFormat,
}

impl CompressionJob {
    fn run(self) -> exn::Result<(), ErrorMessage> {
        let err = || {
            let msg = format!("Failed to compress the directory {:?}", self.root);
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let dir = self
            .root
            .parent()
            .expect("root is a temporary directory, so it has a parent");
        let name = self
            .root
            .file_stem()
            .expect("root is a temporary directory with a name");
        let zip_path = dir.join(format!(
            "{}.{}.cbz",
            name.to_str().expect("our file paths are utf8 compliant"),
            self.target.ext()
        ));

        let file = File::create(zip_path).or_raise(err)?;

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        let mut buffer = Vec::new();
        for entry in WalkDir::new(&self.root).into_iter() {
            let entry = entry.or_raise(err)?;
            let entry = entry.path();
            let root_parent = self
                .root
                .parent()
                .expect("root dir is a temporary directory, not root");
            let inner_path = entry
                .strip_prefix(root_parent)
                .expect("all files have the root as prefix")
                .to_str()
                .expect("path should be utf8 compliant");

            if entry.is_file() {
                zipper.start_file(inner_path, options).or_raise(err)?;
                File::open(entry)
                    .and_then(|mut f| f.read_to_end(&mut buffer))
                    .or_raise(err)?;
                zipper.write_all(&buffer).or_raise(err)?;
                buffer.clear();
            } else if !inner_path.is_empty() {
                zipper.add_directory(inner_path, options).or_raise(err)?;
            }
        }

        zipper.finish().or_raise(err)?;
        Ok(())
    }
}

struct ConversionJobs {
    job_queue: VecDeque<ConversionJob>,
    jobs_in_progress: Vec<Option<ConversionJob>>,
}

impl ConversionJobs {
    fn run(mut self, bar: &ProgressBar) -> exn::Result<(), ErrorMessage> {
        assert!(!self.job_queue.is_empty());
        bar.reset();
        bar.set_length(self.job_queue.len() as u64);

        // these signals will be catched from here on out until dropped
        let mut signals = Signals::new([SIGINT, SIGCHLD]).or_raise(|| {
            let msg = "Could not listen to process signals".to_string();
            debug!("{msg}");
            ErrorMessage(msg)
        })?;

        // start out as many jobs as allowed
        for slot in self.jobs_in_progress.iter_mut() {
            let Some(job) = self.job_queue.pop_front() else {
                break;
            };
            *slot = Some(job);
        }
        self.proceed_jobs(bar)?;

        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => {
                        debug!("got interrupted");
                        return Err(ErrorMessage("Got interrupted".to_string()).raise());
                    }
                    SIGCHLD => self.proceed_jobs(bar)?,
                    _ => unreachable!(),
                }
            }
        }
        Ok(())
    }

    fn proceed_jobs(&mut self, bar: &ProgressBar) -> exn::Result<(), ErrorMessage> {
        for slot in self.jobs_in_progress.iter_mut() {
            loop {
                match slot.take() {
                    Some(job) => match job.proceed()? {
                        Proceeded::SameAsBefore(job) => {
                            *slot = Some(job);
                            break;
                        }
                        Proceeded::Progress(job) => *slot = Some(job),
                        Proceeded::Finished => bar.inc(1),
                    },
                    None => match self.job_queue.pop_front() {
                        Some(job) => *slot = Some(job),
                        None => break,
                    },
                }
            }
        }
        Ok(())
    }

    fn jobs_pending(&self) -> bool {
        self.jobs_in_progress.iter().any(|job| job.is_some())
    }
}

struct ArchiveJob {
    archive_path: ArchivePath,
    extraction: ExtractionJob,
    conversion: ConversionJobs,
    compression: CompressionJob,
}

impl ArchiveJob {
    fn new(
        archive_path: ArchivePath,
        config: ConversionConfig,
    ) -> exn::Result<Result<Self, NothingToDo>, ErrorMessage> {
        let ConversionConfig {
            target, n_workers, ..
        } = config;

        if Self::already_converted(&archive_path, target) {
            return Ok(Err(NothingToDo::AlreadyDone(archive_path.into_inner())));
        }

        let extract_dir = Self::get_conversion_root_dir(&archive_path);
        if extract_dir.exists() {
            let msg = format!("Extract directory already exists at {archive_path:?}");
            debug!("{msg}");
            bail!(ErrorMessage(msg));
        }

        let root_dir = Self::get_extraction_root_dir(&archive_path)?;
        let job_queue = Self::images_in_archive(&archive_path)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let image_path = root_dir.join(image_path);
                match ConversionJobDetails::new(&image_path, format, config) {
                    Ok(Some(task)) => {
                        debug!("create job for {image_path:?}: {task:?}");
                        Some(Ok(ConversionJob::new(image_path, task)))
                    }
                    Ok(None) => {
                        debug!("skip conversion for {image_path:?}");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<VecDeque<_>, _>>()
            .or_raise(|| {
                let msg = "Error while preparing all conversion job".to_string();
                debug!("{msg}");
                ErrorMessage(msg)
            })?;

        if job_queue.is_empty() {
            return Ok(Err(NothingToDo::NoFilesToConvert(
                archive_path.into_inner(),
            )));
        }

        let jobs_in_progress = Vec::from_iter((0..n_workers).map(|_| None));

        let extraction = ExtractionJob {
            archive_path: archive_path.clone(),
        };
        let conversion = ConversionJobs {
            job_queue,
            jobs_in_progress,
        };
        let compression = CompressionJob {
            root: Self::get_conversion_root_dir(&archive_path),
            target,
        };
        Ok(Ok(Self {
            archive_path,
            extraction,
            conversion,
            compression,
        }))
    }

    pub fn run(self, bar: &ProgressBar) -> exn::Result<(), ErrorMessage> {
        let Self {
            archive_path,
            extraction,
            conversion,
            compression,
        } = self;

        let err = || {
            let msg = format!(
                "Error during conversion of the archive {:?}",
                archive_path.deref()
            );
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let _guard = extraction.run().or_raise(err)?;
        conversion.run(bar).or_raise(err)?;
        compression.run().or_raise(err)?;

        Ok(())
    }

    pub fn archive(&self) -> &Path {
        &self.archive_path
    }

    fn get_conversion_root_dir(cbz_path: &Path) -> PathBuf {
        let dir = cbz_path.parent().unwrap();
        let name = cbz_path.file_stem().unwrap();
        dir.join(name)
    }

    fn already_converted(path: &ArchivePath, format: ImageFormat) -> bool {
        let conversion_ending = format!(".{}.cbz", format.ext());

        let dir = path.parent().unwrap();
        let name = path.file_stem().unwrap();
        let zip_path = dir.join(format!("{}{}", name.to_str().unwrap(), conversion_ending));

        let is_converted_archive = path.to_str().unwrap().ends_with(&conversion_ending);
        let has_converted_archive = zip_path.exists();

        is_converted_archive || has_converted_archive
    }

    fn get_extraction_root_dir(cbz_path: &Path) -> exn::Result<PathBuf, ErrorMessage> {
        let err = || {
            let msg = format!("Could not determine the extraction root dir for {cbz_path:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let archive_name = cbz_path.file_stem().unwrap();
        let archive_root_dirs = spawn::list_archive_files(cbz_path)
            .and_then(|c| c.wait_with_output())
            .or_raise(err)?
            .stdout
            .lines()
            .filter(|v| v.as_ref().is_ok_and(|line| line.starts_with("Path = ")))
            .map(|v| v.unwrap().strip_prefix("Path = ").unwrap().to_string())
            .filter(|file| !file.contains("/"))
            .collect::<Vec<_>>();

        let has_root_within =
            archive_root_dirs.len() == 1 && *archive_root_dirs[0] == *archive_name;
        let extract_dir = match has_root_within {
            true => {
                let parent_dir = cbz_path.parent().unwrap().to_path_buf();
                assert_eq!(
                    parent_dir.join(archive_name),
                    Self::get_conversion_root_dir(cbz_path)
                );
                parent_dir
            }
            false => Self::get_conversion_root_dir(cbz_path),
        };
        Ok(extract_dir)
    }

    fn images_in_archive(
        cbz_path: &Path,
    ) -> exn::Result<Vec<(PathBuf, ImageFormat)>, ErrorMessage> {
        let err = || {
            let msg = format!("Could not list files within archive {cbz_path:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };

        spawn::list_archive_files(cbz_path)
            .and_then(|c| c.wait_with_output())
            .or_raise(err)?
            .stdout
            .lines()
            .filter_map(|line| {
                let line = line.or_raise(err);
                let line = match line {
                    Ok(line) => line,
                    Err(e) => return Some(Err(e)),
                };
                let file = line.strip_prefix("Path = ").map(PathBuf::from)?;
                let ext = file.extension()?.to_string_lossy().to_lowercase();

                use ImageFormat::*;
                let file = match ext.as_str() {
                    "jpg" => Some((file, Jpeg)),
                    "jpeg" => Some((file, Jpeg)),
                    "png" => Some((file, Png)),
                    "avif" => Some((file, Avif)),
                    "jxl" => Some((file, Jxl)),
                    "webp" => Some((file, Webp)),
                    _ => None,
                };
                Ok(file).transpose()
            })
            .collect()
    }
}

pub struct SingleArchiveJob(ArchiveJob);

impl SingleArchiveJob {
    pub fn new(
        archive: ArchivePath,
        config: ConversionConfig,
    ) -> exn::Result<Result<Self, NothingToDo>, ErrorMessage> {
        Ok(ArchiveJob::new(archive, config)?.map(Self))
    }

    pub fn run(self, bar: &ProgressBar) -> exn::Result<(), ErrorMessage> {
        self.0.run(bar)
    }
}

pub struct ArchivesInDirectoryJob {
    root_dir: Directory,
    jobs: Vec<ArchiveJob>,
}

pub struct Bars {
    pub multi: MultiProgress,
    pub archives: ProgressBar,
    pub images: ProgressBar,
}

impl ArchivesInDirectoryJob {
    pub fn collect(
        root_dir: Directory,
        config: ConversionConfig,
    ) -> exn::Result<Self, ErrorMessage> {
        let err = || {
            let msg = format!(
                "Error while looking for archives needing conversion from root {root_dir:?}"
            );
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let jobs = root_dir
            .read_dir()
            .or_raise(err)?
            .filter_map(|dir_entry| {
                let path = match dir_entry.or_raise(err) {
                    Ok(dir_entry) => dir_entry.path(),
                    Err(e) => return Some(Err(e)),
                };
                let archive = match ArchivePath::new(path) {
                    Ok(archive) => archive,
                    Err(exn) => {
                        let (path, exn) = exn.recover();
                        debug!("skipping {path:?}: {exn:?}");
                        return None;
                    }
                };

                info!("Checking {:?}", archive.deref());
                match ArchiveJob::new(archive, config) {
                    Ok(Ok(job)) => Some(Ok(job)),
                    Ok(Err(nothing_to_do)) => {
                        info!("{nothing_to_do}");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<exn::Result<Vec<_>, ErrorMessage>>()?;
        Ok(Self { root_dir, jobs })
    }

    pub fn run(self, bars: &Bars) -> exn::Result<(), ErrorMessage> {
        let err = || {
            let msg = format!("Failed for some archive inside {:?}", self.root_dir);
            debug!("{msg}");
            ErrorMessage(msg)
        };

        bars.archives.reset();
        bars.archives.set_length(self.jobs.len() as u64);

        for job in self.jobs.into_iter() {
            info!("Converting {:?}", job.archive());
            // ignore error that may happen when writing to stdout
            let _ = bars
                .multi
                .println(format!("Converting {:?}", job.archive()));
            job.run(&bars.images).or_raise(err)?;
            bars.archives.inc(1);
            info!("Done");
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Directory(PathBuf);

impl Directory {
    pub fn new(path: PathBuf) -> Result<Self, Exn<ErrorMessage, PathBuf>> {
        match path.is_dir() {
            true => Ok(Self(path)),
            false => {
                let msg = format!("Provided path is not a directory: {path:?}");
                debug!("{msg}");
                let err = Exn::with_recovery(ErrorMessage(msg), path);
                Err(err)
            }
        }
    }

    fn into_inner(self) -> PathBuf {
        self.0
    }
}

impl std::ops::Deref for Directory {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::convert::AsRef<Path> for Directory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

struct RecursiveHardLinkJob {
    root: Directory,
    target: ImageFormat,
}

impl RecursiveHardLinkJob {
    fn run(self) -> exn::Result<TempDirGuard, ErrorMessage> {
        let err = || {
            let msg = format!(
                "Error while creating hard links from {:?} to {:?}",
                self.root, self.target
            );
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let copy_root = RecursiveDirJob::get_hardlink_dir(&self.root, self.target)
            .expect("checked by construction that dir is not root");

        let guard = TempDirGuard::new(copy_root.to_path_buf());

        for entry in WalkDir::new(&self.root).same_file_system(true) {
            let entry = entry.or_raise(err)?;
            let path = entry.path();
            let rel_path = path
                .strip_prefix(&self.root)
                .expect("all files have the root as prefix");
            let copy_path = copy_root.join(rel_path);

            if path.is_file() {
                fs::hard_link(path, &copy_path).or_raise(err)?;
            } else if path.is_dir() {
                fs::create_dir(&copy_path).or_raise(err)?;
            }
        }

        Ok(guard)
    }
}

pub struct RecursiveDirJob {
    root: Directory,
    hardlink: RecursiveHardLinkJob,
    conversion: ConversionJobs,
}

impl RecursiveDirJob {
    pub fn new(
        root: Directory,
        config: ConversionConfig,
    ) -> exn::Result<Result<Self, NothingToDo>, ErrorMessage> {
        let err = || {
            let msg = format!(
                "Failed to prepare job for recursive image conversion starting at {root:?}"
            );
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let ConversionConfig {
            target, n_workers, ..
        } = config;

        let copy_root = Self::get_hardlink_dir(&root, config.target).or_raise(err)?;
        let job_queue = Self::images_in_dir(&root)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let rel_path = image_path
                    .strip_prefix(&root)
                    .expect("image path is within root by construction");
                let copy_path = copy_root.join(rel_path);
                match ConversionJobDetails::new(&copy_path, format, config) {
                    Ok(Some(task)) => {
                        debug!("create job for {copy_path:?}: {task:?}");
                        Some(Ok(ConversionJob::new(copy_path, task)))
                    }
                    Ok(None) => {
                        debug!("skip conversion for {copy_path:?}");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<VecDeque<_>, _>>()
            .or_raise(err)?;
        if job_queue.is_empty() {
            return Ok(Err(NothingToDo::NoFilesToConvert(root.into_inner())));
        }

        let jobs_in_progress = Vec::from_iter((0..n_workers).map(|_| None));

        let hardlink = RecursiveHardLinkJob {
            root: root.clone(),
            target,
        };
        let conversion = ConversionJobs {
            job_queue,
            jobs_in_progress,
        };
        Ok(Ok(Self {
            root,
            hardlink,
            conversion,
        }))
    }

    pub fn run(self, bar: &ProgressBar) -> exn::Result<(), ErrorMessage> {
        let Self {
            root,
            hardlink,
            conversion,
        } = self;

        let err = || {
            let msg = format!("Failed to convert all images recursively in {root:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let guard = hardlink.run().or_raise(err)?;
        conversion.run(bar).or_raise(err)?;

        guard.keep();
        Ok(())
    }

    fn images_in_dir(root: &Directory) -> exn::Result<Vec<(PathBuf, ImageFormat)>, ErrorMessage> {
        let err = || {
            let msg = format!("Could not list images recursively in the directory {root:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };

        WalkDir::new(root)
            .into_iter()
            .filter_map(|entry| {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(inner) => {
                        let outer = err();
                        return Some(Err(inner.raise().raise(outer)));
                    }
                };
                let file = entry.path().to_path_buf();
                let ext = file.extension()?.to_string_lossy().to_lowercase();

                use ImageFormat::*;
                let file = match ext.as_str() {
                    "jpg" => Some((file, Jpeg)),
                    "jpeg" => Some((file, Jpeg)),
                    "png" => Some((file, Png)),
                    "avif" => Some((file, Avif)),
                    "jxl" => Some((file, Jxl)),
                    "webp" => Some((file, Webp)),
                    _ => None,
                };
                Ok(file).transpose()
            })
            .collect()
    }

    fn get_hardlink_dir(
        root: &Directory,
        target: ImageFormat,
    ) -> exn::Result<PathBuf, ErrorMessage> {
        let err = || {
            let msg = format!("Directory has no parent: {root:?}");
            debug!("{msg}");
            ErrorMessage(msg)
        };
        let parent = root.parent().ok_or_raise(err)?;
        let name = root.file_stem().unwrap().to_string_lossy();
        let new_name = format!("{}-{}", name, target.ext());
        Ok(parent.join(new_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_for_compressed_jxl() {
        let compressed_path = PathBuf::from("test_data/compressed.jxl");
        assert!(compressed_path.exists());
        let out = ConversionJobDetails::jxl_is_compressed_jpeg(&compressed_path).unwrap();
        assert!(out);
    }

    #[test]
    fn test_check_for_encoded_jxl() {
        let encoded_path = PathBuf::from("test_data/encoded.jxl");
        assert!(encoded_path.exists());
        let out = ConversionJobDetails::jxl_is_compressed_jpeg(&encoded_path).unwrap();
        assert!(!out);
    }
}
