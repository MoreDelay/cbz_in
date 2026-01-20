use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use indicatif::{MultiProgress, ProgressBar};
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use tracing::{debug, error, info};
use walkdir::WalkDir;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::spawn;

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

#[derive(Error, Debug)]
pub enum InvalidArchivePath {
    #[error("The provided path does not exist: '{0}'")]
    DoesNotExist(PathBuf),
    #[error("This is not an archive: '{0}'")]
    WrongExtension(PathBuf),
}

impl ArchivePath {
    const ARCHIVE_EXTENSIONS: [&str; 2] = ["zip", "cbz"];

    pub fn validate(archive_path: PathBuf) -> Result<Self, InvalidArchivePath> {
        if !archive_path.is_file() {
            return Err(InvalidArchivePath::DoesNotExist(archive_path));
        }

        let correct_extension = archive_path.extension().is_some_and(|ext| {
            Self::ARCHIVE_EXTENSIONS
                .iter()
                .any(|valid_ext| ext.eq_ignore_ascii_case(valid_ext))
        });
        if !correct_extension {
            return Err(InvalidArchivePath::WrongExtension(archive_path));
        }
        Ok(ArchivePath(archive_path))
    }

    fn into_inner(self) -> PathBuf {
        self.0
    }
}

#[derive(Debug)]
struct ConversionDirect {
    current: ImageFormat,
    target: ImageFormat,
}
#[derive(Debug)]
struct ConversionIntermediate {
    current: ImageFormat,
    inbetween: ImageFormat,
    target: ImageFormat,
}
#[derive(Debug)]
enum ConversionJobDetails {
    Direct(ConversionDirect),
    Intermediate(ConversionIntermediate),
}

#[derive(Debug, Clone, Copy)]
pub struct ConversionConfig {
    pub target: ImageFormat,
    pub n_workers: usize,
    pub forced: bool,
}

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("Could not query jxl file '{0}'")]
    Jxlinfo(PathBuf, #[source] spawn::ProcessError),
}

impl ConversionJobDetails {
    fn new(
        image_path: &Path,
        current: ImageFormat,
        config: ConversionConfig,
    ) -> Result<Option<Self>, TaskError> {
        use ImageFormat::*;

        let ConversionConfig { target, forced, .. } = config;

        let out = match (current, target) {
            (a, b) if a == b => return Ok(None),
            (Avif, Jxl | Webp) => Self::Intermediate(ConversionIntermediate {
                current,
                inbetween: Png,
                target,
            }),
            (Jxl, Avif | Webp) => match Self::jxl_is_compressed_jpeg(image_path)? {
                true => Self::Intermediate(ConversionIntermediate {
                    current,
                    inbetween: Jpeg,
                    target,
                }),
                false => Self::Intermediate(ConversionIntermediate {
                    current,
                    inbetween: Png,
                    target,
                }),
            },
            (Webp, Jpeg | Avif | Jxl) => Self::Intermediate(ConversionIntermediate {
                current,
                inbetween: Png,
                target,
            }),
            (_, _) => Self::Direct(ConversionDirect { current, target }),
        };
        let perform = forced || out.perform_always();
        Ok(perform.then_some(out))
    }

    fn perform_always(&self) -> bool {
        let tuple = match self {
            ConversionJobDetails::Direct(ConversionDirect { current, target }) => {
                (*current, *target)
            }
            ConversionJobDetails::Intermediate(ConversionIntermediate {
                current, target, ..
            }) => (*current, *target),
        };

        use ImageFormat::*;
        match tuple {
            (Jpeg | Png, _) => true,
            (_, Jpeg | Png) => true,
            (_, _) => false,
        }
    }

    fn jxl_is_compressed_jpeg(image_path: &Path) -> Result<bool, TaskError> {
        let has_box = spawn::run_jxlinfo(image_path)
            .and_then(|c| c.wait_with_output())
            .map_err(|e| TaskError::Jxlinfo(image_path.to_path_buf(), e))?
            .stdout
            .lines()
            .any(|line| line.unwrap().starts_with("box: type: \"jbrd\""));
        Ok(has_box)
    }
}

#[derive(Debug)]
struct ConversionJobWaiting {
    image_path: PathBuf,
    details: ConversionJobDetails,
}
#[derive(Debug)]
struct ConversionJobRunning {
    child: spawn::ManagedChild,
    image_path: PathBuf,
    after: Option<ConversionDirect>,
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

#[derive(Error, Debug)]
pub enum ConversionJobError {
    #[error("An error occurred in a conversion process for image '{0}'")]
    Process(PathBuf, #[source] spawn::ProcessError),
    #[error("An error occurred while waiting for a conversion process for image '{0}'")]
    Wait(PathBuf, #[source] spawn::ProcessError),
    #[error("Could not delete the file: '{0}'")]
    DeleteFile(PathBuf, #[source] std::io::Error),
}

impl ConversionJobWaiting {
    fn start_conversion(self) -> Result<ConversionJobRunning, ConversionJobError> {
        let ConversionJobWaiting {
            image_path,
            details: task,
        } = self;
        let (current, target, after) = match task {
            ConversionJobDetails::Direct(ConversionDirect { current, target }) => {
                (current, target, None)
            }
            ConversionJobDetails::Intermediate(ConversionIntermediate {
                current,
                inbetween,
                target,
            }) => (
                current,
                inbetween,
                Some(ConversionDirect {
                    current: inbetween,
                    target,
                }),
            ),
        };
        let input_path = &image_path;
        let output_path = &image_path.with_extension(target.ext());

        let map = |e| ConversionJobError::Process(image_path.to_path_buf(), e);

        use ImageFormat::*;
        let child = match (current, target) {
            (Jpeg, Png) => spawn::convert_jpeg_to_png(input_path, output_path).map_err(map)?,
            (Png, Jpeg) => spawn::convert_png_to_jpeg(input_path, output_path).map_err(map)?,
            (Jpeg | Png, Avif) => spawn::encode_avif(input_path, output_path).map_err(map)?,
            (Jpeg | Png, Jxl) => spawn::encode_jxl(input_path, output_path).map_err(map)?,
            (Jpeg | Png, Webp) => spawn::encode_webp(input_path, output_path).map_err(map)?,
            (Avif, Jpeg) => spawn::decode_avif_to_jpeg(input_path, output_path).map_err(map)?,
            (Avif, Png) => spawn::decode_avif_to_png(input_path, output_path).map_err(map)?,
            (Jxl, Jpeg) => spawn::decode_jxl_to_jpeg(input_path, output_path).map_err(map)?,
            (Jxl, Png) => spawn::decode_jxl_to_png(input_path, output_path).map_err(map)?,
            (Webp, Png) => spawn::decode_webp(input_path, output_path).map_err(map)?,
            (_, _) => unreachable!(),
        };

        Ok(ConversionJobRunning {
            child,
            image_path,
            after,
        })
    }
}

impl ConversionJobRunning {
    fn child_done(&mut self) -> Result<bool, ConversionJobError> {
        self.child
            .try_wait()
            .map_err(|e| ConversionJobError::Wait(self.image_path.to_path_buf(), e))
    }
}

impl ConversionJobCompleted {
    /// wait on child process and delete original image file
    fn complete(self) -> Result<Option<ConversionJobWaiting>, ConversionJobError> {
        let Self(ConversionJobRunning {
            child,
            image_path,
            after,
        }) = self;

        child
            .wait()
            .map_err(|e| ConversionJobError::Process(image_path.to_path_buf(), e))?;
        if let Err(err) = fs::remove_file(&image_path) {
            return Err(ConversionJobError::DeleteFile(image_path, err));
        };

        let after = after.map(|direct| ConversionJobWaiting {
            image_path: image_path.with_extension(direct.current.ext()),
            details: ConversionJobDetails::Direct(direct),
        });
        Ok(after)
    }
}

impl ConversionJob {
    fn new(image_path: PathBuf, task: ConversionJobDetails) -> Self {
        let waiting = ConversionJobWaiting {
            image_path,
            details: task,
        };
        Self::Waiting(waiting)
    }

    fn proceed(self) -> Result<Proceeded, ConversionJobError> {
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
            ConversionJob::Completed(completed) => match completed.complete()? {
                Some(waiting) => Proceeded::Progress(Self::Waiting(waiting)),
                None => Proceeded::Finished,
            },
        };
        Ok(proceeded)
    }
}

struct ExtractionJob {
    archive_path: ArchivePath,
}

#[derive(Debug, Error)]
pub enum ExtractionError {
    #[error("Failed to create temporary directory for archive extraction at '{0}'")]
    TempDir(PathBuf, #[source] std::io::Error),
    #[error("An error occurred while extracting archive '{0}'")]
    Process(PathBuf, #[source] spawn::ProcessError),
}

impl ExtractionJob {
    fn run(self) -> Result<TempDirGuard, ExtractionError> {
        assert!(self.archive_path.is_file());

        let extract_dir = ArchiveJob::get_conversion_root_dir(&self.archive_path);
        assert!(!extract_dir.exists());

        let guard = TempDirGuard::new(extract_dir.to_path_buf());
        fs::create_dir_all(&extract_dir)
            .map_err(|e| ExtractionError::TempDir(extract_dir.to_path_buf(), e))?;
        spawn::extract_zip(&self.archive_path, &extract_dir)
            .and_then(|c| c.wait())
            .map_err(|e| ExtractionError::Process(self.archive_path.to_path_buf(), e))?;
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
        if let Some(root) = &self.temp_root
            && root.exists()
            && let Err(e) = fs::remove_dir_all(root)
        {
            error!("error on deleting directory {root:?}: {e}");
        }
    }
}

struct CompressionJob {
    root: PathBuf,
    target: ImageFormat,
}

#[derive(Debug, Error)]
pub enum CompressionError {
    #[error("Error while creating archive '{0}'")]
    Zipping(PathBuf, #[source] zip::result::ZipError),
    #[error("Encountered error while walking the temporary directory for archive '{0}'")]
    WalkTempDir(PathBuf, #[source] walkdir::Error),
}

impl CompressionJob {
    fn run(self) -> Result<(), CompressionError> {
        // error mapping helpers
        let from_zip = |e| CompressionError::Zipping(self.root.to_path_buf(), e);
        let from_io = |e| from_zip(zip::result::ZipError::Io(e));

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

        let file = File::create(zip_path).map_err(from_io)?;

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        let mut buffer = Vec::new();
        for entry in WalkDir::new(&self.root).into_iter() {
            let entry =
                entry.map_err(|e| CompressionError::WalkTempDir(self.root.to_path_buf(), e))?;
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
                zipper.start_file(inner_path, options).map_err(from_zip)?;
                File::open(entry)
                    .and_then(|mut f| f.read_to_end(&mut buffer))
                    .map_err(from_io)?;
                zipper.write_all(&buffer).map_err(from_io)?;
                buffer.clear();
            } else if !inner_path.is_empty() {
                zipper
                    .add_directory(inner_path, options)
                    .map_err(from_zip)?;
            }
        }

        zipper.finish().map_err(from_zip)?;
        Ok(())
    }
}

struct ConversionJobs {
    job_queue: VecDeque<ConversionJob>,
    jobs_in_progress: Vec<Option<ConversionJob>>,
}

#[derive(Debug, Error)]
pub enum ConversionJobsError {
    #[error("Could not listen to process signals")]
    Signals(#[from] std::io::Error),
    #[error("Error during conversion of an image")]
    ConversionJob(#[from] ConversionJobError),
    #[error("Got interrupted")]
    Interrupt,
}

impl ConversionJobs {
    fn run(mut self, bar: &ProgressBar) -> Result<(), ConversionJobsError> {
        assert!(!self.job_queue.is_empty());
        bar.reset();
        bar.set_length(self.job_queue.len() as u64);

        // these signals will be catched from here on out until dropped
        let mut signals = Signals::new([SIGINT, SIGCHLD])?;

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
                    SIGINT => return Err(ConversionJobsError::Interrupt),
                    SIGCHLD => self.proceed_jobs(bar)?,
                    _ => unreachable!(),
                }
            }
        }
        Ok(())
    }

    fn proceed_jobs(&mut self, bar: &ProgressBar) -> Result<(), ConversionJobError> {
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

#[derive(Debug, Error)]
pub enum ArchiveJobCreateError {
    #[error("Invalid archive")]
    InvalidArchive(#[from] InvalidArchivePath),
    #[error("Extract directory already exists at '{0}', delete it and try again")]
    ExtractDirExists(PathBuf),
    #[error("An error occurred while reading archive files for '{0}'")]
    ListingFiles(PathBuf, #[source] spawn::ProcessError),
    #[error("An error occurred while reading archive files for '{0}'")]
    ListingFile(PathBuf, #[source] std::io::Error),
    #[error("Error while creating conversion tasks for archive '{0}'")]
    Task(PathBuf, #[source] TaskError),
}

#[derive(Debug, Error)]
pub enum ArchiveJobRunError {
    #[error("Error while extracting for archive '{0}'")]
    Extraction(PathBuf, #[source] ExtractionError),
    #[error("Error while performing conversions for archive '{0}'")]
    Conversion(PathBuf, #[source] ConversionJobsError),
    #[error("Error while compressing for archive '{0}'")]
    Compression(PathBuf, #[source] CompressionError),
}

impl ArchiveJob {
    pub fn new(
        archive: PathBuf,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo>, ArchiveJobCreateError> {
        let archive = ArchivePath::validate(archive)?;
        Self::from_validated(archive, config)
    }

    pub fn run(self, bar: &ProgressBar) -> Result<(), ArchiveJobRunError> {
        let Self {
            archive_path,
            extraction,
            conversion,
            compression,
        } = self;

        let _guard = extraction
            .run()
            .map_err(|e| ArchiveJobRunError::Extraction(archive_path.to_path_buf(), e))?;
        conversion
            .run(bar)
            .map_err(|e| ArchiveJobRunError::Conversion(archive_path.to_path_buf(), e))?;
        compression
            .run()
            .map_err(|e| ArchiveJobRunError::Compression(archive_path.to_path_buf(), e))?;

        Ok(())
    }

    pub fn archive(&self) -> &Path {
        &self.archive_path
    }

    fn from_validated(
        archive_path: ArchivePath,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo>, ArchiveJobCreateError> {
        let ConversionConfig {
            target, n_workers, ..
        } = config;

        if Self::already_converted(&archive_path, target) {
            return Ok(Err(NothingToDo::AlreadyDone(archive_path.into_inner())));
        }

        let extract_dir = Self::get_conversion_root_dir(&archive_path);
        if extract_dir.exists() {
            return Err(ArchiveJobCreateError::ExtractDirExists(extract_dir));
        }

        let root_dir = Self::get_extraction_root_dir(&archive_path)?;
        let job_queue = Self::images_in_archive(&archive_path)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let image_path = root_dir.join(image_path);
                match ConversionJobDetails::new(&image_path, format, config) {
                    Ok(Some(task)) => Some(Ok(ConversionJob::new(image_path, task))),
                    Ok(None) => {
                        debug!("skip conversion for '{image_path:?}'");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<VecDeque<_>, _>>()
            .map_err(|e| ArchiveJobCreateError::Task(archive_path.to_path_buf(), e))?;

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

    fn get_extraction_root_dir(cbz_path: &Path) -> Result<PathBuf, ArchiveJobCreateError> {
        let archive_name = cbz_path.file_stem().unwrap();
        let archive_root_dirs = spawn::list_archive_files(cbz_path)
            .and_then(|c| c.wait_with_output())
            .map_err(|e| ArchiveJobCreateError::ListingFiles(cbz_path.to_path_buf(), e))?
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
    ) -> Result<Vec<(PathBuf, ImageFormat)>, ArchiveJobCreateError> {
        spawn::list_archive_files(cbz_path)
            .and_then(|c| c.wait_with_output())
            .map_err(|e| ArchiveJobCreateError::ListingFiles(cbz_path.to_path_buf(), e))?
            .stdout
            .lines()
            .filter_map(|line| {
                let line =
                    line.map_err(|e| ArchiveJobCreateError::ListingFile(cbz_path.to_path_buf(), e));
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

#[derive(Error, Debug)]
pub enum SingleArchiveJobError {
    #[error("Could not create an archive conversion job")]
    Create(#[from] ArchiveJobCreateError),
    #[error("Could not run an archive conversion job")]
    Run(#[from] ArchiveJobRunError),
}

impl SingleArchiveJob {
    pub fn new(
        archive: PathBuf,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo>, SingleArchiveJobError> {
        Ok(ArchiveJob::new(archive, config)?.map(Self))
    }

    pub fn run(self, bar: &ProgressBar) -> Result<(), SingleArchiveJobError> {
        Ok(self.0.run(bar)?)
    }
}

pub struct ArchivesInDirectoryJob(Vec<ArchiveJob>);

#[derive(Error, Debug)]
pub enum ArchivesInDirectoryJobError {
    #[error("Could not create an archive conversion job")]
    ArchiveJobCreate(#[from] ArchiveJobCreateError),
    #[error("Could not run an archive conversion job")]
    ArchiveJobRun(#[from] ArchiveJobRunError),
    #[error("Could not walk the filesystem")]
    ReadingDir(#[from] std::io::Error),
}

pub struct Bars {
    pub multi: MultiProgress,
    pub archives: ProgressBar,
    pub images: ProgressBar,
}

impl ArchivesInDirectoryJob {
    pub fn collect(
        root_dir: &Path,
        config: ConversionConfig,
    ) -> Result<Self, ArchivesInDirectoryJobError> {
        let jobs = root_dir
            .read_dir()?
            .filter_map(|dir_entry| {
                let path = match dir_entry {
                    Ok(dir_entry) => dir_entry.path(),
                    Err(e) => return Some(Err(e.into())),
                };
                let archive = match ArchivePath::validate(path) {
                    Ok(f) => f,
                    Err(e) => {
                        debug!("skipping: {e}");
                        return None;
                    }
                };

                info!("Checking {:?}", archive.deref());
                match ArchiveJob::from_validated(archive, config) {
                    Ok(Ok(job)) => Some(Ok(job)),
                    Ok(Err(nothing_to_do)) => {
                        info!("{nothing_to_do}");
                        None
                    }
                    Err(e) => Some(Err(e.into())),
                }
            })
            .collect::<Result<Vec<_>, ArchivesInDirectoryJobError>>()?;
        Ok(Self(jobs))
    }

    pub fn run(self, bars: &Bars) -> Result<(), ArchivesInDirectoryJobError> {
        bars.archives.reset();
        bars.archives.set_length(self.0.len() as u64);

        for job in self.0.into_iter() {
            info!("Converting {:?}", job.archive());
            bars.multi
                .println(format!("Converting {:?}", job.archive()))?;
            job.run(&bars.images)?;
            bars.archives.inc(1);
            info!("Done");
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Directory(PathBuf);

impl Directory {
    fn new(path: PathBuf) -> Result<Self, PathBuf> {
        match path.is_dir() {
            true => Ok(Self(path)),
            false => Err(path),
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

#[derive(Debug, Error)]
pub enum RecursiveHardLinkJobError {
    #[error("Encountered error while walking the directory '{0}'")]
    WalkDir(PathBuf, #[source] walkdir::Error),
    #[error("Could not create a hardlink for file '{0}'")]
    Hardlink(PathBuf, #[source] std::io::Error),
    #[error("Could not create the mirrored directory '{0}'")]
    CreateDir(PathBuf, #[source] std::io::Error),
}

impl RecursiveHardLinkJob {
    fn run(self) -> Result<TempDirGuard, RecursiveHardLinkJobError> {
        let copy_root = RecursiveDirJob::get_hardlink_dir(&self.root, self.target)
            .expect("checked by construction that dir is not root");

        let guard = TempDirGuard::new(copy_root.to_path_buf());

        for entry in WalkDir::new(&self.root).same_file_system(true) {
            let entry = entry
                .map_err(|e| RecursiveHardLinkJobError::WalkDir(self.root.to_path_buf(), e))?;
            let path = entry.path();
            let rel_path = path
                .strip_prefix(&self.root)
                .expect("all files have the root as prefix");
            let copy_path = copy_root.join(rel_path);

            if path.is_file() {
                fs::hard_link(path, &copy_path)
                    .map_err(|e| RecursiveHardLinkJobError::Hardlink(copy_path.to_path_buf(), e))?;
            } else if path.is_dir() {
                fs::create_dir(&copy_path).map_err(|e| {
                    RecursiveHardLinkJobError::CreateDir(copy_path.to_path_buf(), e)
                })?;
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

#[derive(Debug, Error)]
pub enum RecursiveDirJobError {
    #[error("Provided root path is not a directory: '{0}'")]
    NotDir(PathBuf),
    #[error("Can not create a hardlink copy of root")]
    Root,
    #[error("Encountered error while walking the directory '{0}'")]
    WalkDir(PathBuf, #[source] walkdir::Error),
    #[error("Error while creating conversion tasks for archive '{0}'")]
    Task(PathBuf, #[source] TaskError),
    #[error("Could not create a copied directory structure with hardlinks for '{0}'")]
    Hardlink(PathBuf, #[source] RecursiveHardLinkJobError),
    #[error("Error while performing conversions within root directory '{0}'")]
    Conversion(PathBuf, #[source] ConversionJobsError),
}

impl RecursiveDirJob {
    pub fn new(
        root: PathBuf,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo>, RecursiveDirJobError> {
        let ConversionConfig {
            target, n_workers, ..
        } = config;

        let root = Directory::new(root).map_err(|root| RecursiveDirJobError::NotDir(root))?;
        let copy_root =
            Self::get_hardlink_dir(&root, config.target).ok_or(RecursiveDirJobError::Root)?;
        let job_queue = Self::images_in_dir(&root)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let rel_path = image_path
                    .strip_prefix(&root)
                    .expect("image path is within root by construction");
                let copy_path = copy_root.join(rel_path);
                match ConversionJobDetails::new(&copy_path, format, config) {
                    Ok(Some(task)) => Some(Ok(ConversionJob::new(copy_path, task))),
                    Ok(None) => {
                        debug!("skip conversion for '{copy_path:?}'");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<VecDeque<_>, _>>()
            .map_err(|e| RecursiveDirJobError::Task(root.to_path_buf(), e))?;
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

    pub fn run(self, bar: &ProgressBar) -> Result<(), RecursiveDirJobError> {
        let Self {
            root,
            hardlink,
            conversion,
        } = self;

        let guard = hardlink
            .run()
            .map_err(|e| RecursiveDirJobError::Hardlink(root.to_path_buf(), e))?;
        conversion
            .run(bar)
            .map_err(|e| RecursiveDirJobError::Conversion(root.to_path_buf(), e))?;

        guard.keep();
        Ok(())
    }

    fn images_in_dir(
        root: &Directory,
    ) -> Result<Vec<(PathBuf, ImageFormat)>, RecursiveDirJobError> {
        WalkDir::new(root)
            .into_iter()
            .filter_map(|entry| {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        return Some(Err(RecursiveDirJobError::WalkDir(root.to_path_buf(), e)));
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

    fn get_hardlink_dir(root: &Directory, target: ImageFormat) -> Option<PathBuf> {
        let parent = root.parent()?;
        let name = root.file_stem().unwrap().to_string_lossy();
        let new_name = format!("{}-{}", name, target.ext());
        Some(parent.join(new_name))
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
