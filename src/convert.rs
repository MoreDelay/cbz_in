use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use indicatif::ProgressBar;
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
enum ConversionTask {
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

impl ConversionTask {
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
            ConversionTask::Direct(ConversionDirect { current, target }) => (*current, *target),
            ConversionTask::Intermediate(ConversionIntermediate {
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
struct ImageJobWaiting {
    image_path: PathBuf,
    task: ConversionTask,
}
#[derive(Debug)]
struct ImageJobRunning {
    child: spawn::ManagedChild,
    image_path: PathBuf,
    after: Option<ConversionDirect>,
}
#[derive(Debug)]
struct ImageJobCompleted(ImageJobRunning);
#[derive(Debug)]
enum ImageJob {
    Waiting(ImageJobWaiting),
    Running(ImageJobRunning),
    Completed(ImageJobCompleted),
}

#[derive(Debug, Error)]
pub enum NothingToDo {
    #[error("No files to convert for '{0}'")]
    NoFilesToConvert(PathBuf),
    #[error("Already converted '{0}'")]
    AlreadyDone(PathBuf),
}

#[derive(Debug)]
enum Proceeded {
    SameAsBefore(ImageJob),
    Progress(ImageJob),
    Finished,
}

#[derive(Error, Debug)]
pub enum ImageJobError {
    #[error("An error occurred in a conversion process for image '{0}'")]
    Process(PathBuf, #[source] spawn::ProcessError),
    #[error("An error occurred while waiting for a conversion process for image '{0}'")]
    Wait(PathBuf, #[source] spawn::ProcessError),
    #[error("Could not delete the file: '{0}'")]
    DeleteFile(PathBuf, #[source] std::io::Error),
}

impl ImageJobWaiting {
    fn start_conversion(self) -> Result<ImageJobRunning, ImageJobError> {
        let ImageJobWaiting { image_path, task } = self;
        let (current, target, after) = match task {
            ConversionTask::Direct(ConversionDirect { current, target }) => (current, target, None),
            ConversionTask::Intermediate(ConversionIntermediate {
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

        let map = |e| ImageJobError::Process(image_path.to_path_buf(), e);

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

        Ok(ImageJobRunning {
            child,
            image_path,
            after,
        })
    }
}

impl ImageJobRunning {
    fn child_done(&mut self) -> Result<bool, ImageJobError> {
        self.child
            .try_wait()
            .map_err(|e| ImageJobError::Wait(self.image_path.to_path_buf(), e))
    }
}

impl ImageJobCompleted {
    /// wait on child process and delete original image file
    fn complete(self) -> Result<Option<ImageJobWaiting>, ImageJobError> {
        let Self(ImageJobRunning {
            child,
            image_path,
            after,
        }) = self;

        child
            .wait()
            .map_err(|e| ImageJobError::Process(image_path.to_path_buf(), e))?;
        if let Err(err) = fs::remove_file(&image_path) {
            return Err(ImageJobError::DeleteFile(image_path, err));
        };

        let after = after.map(|direct| ImageJobWaiting {
            image_path: image_path.with_extension(direct.current.ext()),
            task: ConversionTask::Direct(direct),
        });
        Ok(after)
    }
}

impl ImageJob {
    fn new(image_path: PathBuf, task: ConversionTask) -> Self {
        let waiting = ImageJobWaiting { image_path, task };
        Self::Waiting(waiting)
    }

    fn proceed(self) -> Result<Proceeded, ImageJobError> {
        let proceeded = match self {
            ImageJob::Waiting(waiting) => {
                let running = waiting.start_conversion()?;
                Proceeded::Progress(Self::Running(running))
            }
            ImageJob::Running(mut running) => match running.child_done()? {
                false => Proceeded::SameAsBefore(Self::Running(running)),
                true => {
                    let completed = ImageJobCompleted(running);
                    Proceeded::Progress(Self::Completed(completed))
                }
            },
            ImageJob::Completed(completed) => match completed.complete()? {
                Some(waiting) => Proceeded::Progress(Self::Waiting(waiting)),
                None => Proceeded::Finished,
            },
        };
        Ok(proceeded)
    }
}

pub struct ArchiveJob {
    archive_path: ArchivePath,
    job_queue: VecDeque<ImageJob>,
    jobs_in_progress: Vec<Option<ImageJob>>,
    target: ImageFormat,
}

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("Invalid archive")]
    InvalidArchive(#[from] InvalidArchivePath),
    #[error("Extract directory already exists at '{0}', delete it and try again")]
    ExtractDirExists(PathBuf),
    #[error("Could not listen to process signals before processing '{0}'")]
    Signals(PathBuf, #[source] std::io::Error),
    #[error("Failed to create temporary directory for archive extraction at '{0}'")]
    TempDir(PathBuf, #[source] std::io::Error),
    #[error("Encountered error while walking the temporary directory for archive '{0}'")]
    WalkTempDir(PathBuf, #[source] walkdir::Error),
    #[error("An error occurred while extracting archive '{0}'")]
    Extracting(PathBuf, #[source] spawn::ProcessError),
    #[error("An error occurred while reading archive files for '{0}'")]
    ListingFiles(PathBuf, #[source] spawn::ProcessError),
    #[error("Error while creating conversion tasks for archive '{0}'")]
    Task(PathBuf, #[source] TaskError),
    #[error("Error during conversion of an image in archive '{0}'")]
    ImageJob(PathBuf, #[source] ImageJobError),
    #[error("Error while creating archive '{0}'")]
    Zipping(PathBuf, #[source] zip::result::ZipError),
    #[error("Got interrupted while converting '{0}'")]
    Interrupt(PathBuf),
}

impl ArchiveJob {
    pub fn new(
        archive: PathBuf,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo>, ArchiveError> {
        let archive = ArchivePath::validate(archive)?;
        Self::from_validated(archive, config)
    }

    pub fn run(mut self, bar: &ProgressBar) -> Result<(), ArchiveError> {
        assert!(!self.job_queue.is_empty());
        bar.reset();
        bar.set_length(self.job_queue.len() as u64);

        // these signals will be catched from here on out until dropped
        let mut signals = Signals::new([SIGINT, SIGCHLD])
            .map_err(|e| ArchiveError::Signals(self.archive_path.to_path_buf(), e))?;
        self.extract_cbz()?;

        // start out as many jobs as allowed
        for slot in self.jobs_in_progress.iter_mut() {
            let Some(job) = self.job_queue.pop_front() else {
                break;
            };
            *slot = Some(job);
        }
        self.proceed_jobs(bar)
            .map_err(|e| ArchiveError::ImageJob(self.archive_path.to_path_buf(), e))?;

        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => return Err(ArchiveError::Interrupt(self.archive_path.to_path_buf())),
                    SIGCHLD => self
                        .proceed_jobs(bar)
                        .map_err(|e| ArchiveError::ImageJob(self.archive_path.to_path_buf(), e))?,
                    _ => unreachable!(),
                }
            }
        }
        drop(signals);

        self.compress_cbz()?;
        bar.finish();
        Ok(())
    }

    pub fn archive(&self) -> &Path {
        &self.archive_path
    }

    fn from_validated(
        archive_path: ArchivePath,
        config: ConversionConfig,
    ) -> Result<Result<Self, NothingToDo>, ArchiveError> {
        let ConversionConfig {
            target, n_workers, ..
        } = config;

        if Self::already_converted(&archive_path, target) {
            return Ok(Err(NothingToDo::AlreadyDone(archive_path.into_inner())));
        }

        let extract_dir = Self::get_conversion_root_dir(&archive_path);
        if extract_dir.exists() {
            return Err(ArchiveError::ExtractDirExists(extract_dir));
        }

        let root_dir = Self::get_extraction_root_dir(&archive_path)?;
        let job_queue = Self::images_in_archive(&archive_path)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let image_path = root_dir.join(image_path);
                match ConversionTask::new(&image_path, format, config) {
                    Ok(Some(task)) => Some(Ok(ImageJob::new(image_path, task))),
                    Ok(None) => {
                        debug!("skip conversion for '{image_path:?}'");
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<VecDeque<_>, _>>()
            .map_err(|e| ArchiveError::Task(archive_path.to_path_buf(), e))?;

        if job_queue.is_empty() {
            return Ok(Err(NothingToDo::NoFilesToConvert(
                archive_path.into_inner(),
            )));
        }

        let jobs_in_progress = Vec::from_iter((0..n_workers).map(|_| None));
        Ok(Ok(Self {
            archive_path,
            job_queue,
            jobs_in_progress,
            target,
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

    fn get_extraction_root_dir(cbz_path: &Path) -> Result<PathBuf, ArchiveError> {
        let archive_name = cbz_path.file_stem().unwrap();
        let archive_root_dirs = spawn::list_archive_files(cbz_path)
            .and_then(|c| c.wait_with_output())
            .map_err(|e| ArchiveError::ListingFiles(cbz_path.to_path_buf(), e))?
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

    fn images_in_archive(cbz_path: &Path) -> Result<Vec<(PathBuf, ImageFormat)>, ArchiveError> {
        let files = spawn::list_archive_files(cbz_path)
            .and_then(|c| c.wait_with_output())
            .map_err(|e| ArchiveError::ListingFiles(cbz_path.to_path_buf(), e))?
            .stdout
            .lines()
            .filter_map(|line| {
                let file = line.ok()?.strip_prefix("Path = ").map(PathBuf::from)?;
                let ext = file.extension()?.to_string_lossy().to_lowercase();

                use ImageFormat::*;
                match ext.as_str() {
                    "jpg" => Some((file, Jpeg)),
                    "jpeg" => Some((file, Jpeg)),
                    "png" => Some((file, Png)),
                    "avif" => Some((file, Avif)),
                    "jxl" => Some((file, Jxl)),
                    "webp" => Some((file, Webp)),
                    _ => None,
                }
            })
            .collect();
        Ok(files)
    }

    fn extract_cbz(&mut self) -> Result<(), ArchiveError> {
        assert!(self.archive_path.is_file());

        let extract_dir = Self::get_conversion_root_dir(&self.archive_path);
        assert!(!extract_dir.exists());

        fs::create_dir_all(&extract_dir)
            .map_err(|e| ArchiveError::TempDir(self.archive_path.to_path_buf(), e))?;
        spawn::extract_zip(&self.archive_path, &extract_dir)
            .and_then(|c| c.wait())
            .map_err(|e| ArchiveError::Extracting(self.archive_path.to_path_buf(), e))?;
        Ok(())
    }

    fn compress_cbz(&mut self) -> Result<(), ArchiveError> {
        // error mapping helpers
        let from_zip = |e| ArchiveError::Zipping(self.archive_path.to_path_buf(), e);
        let from_io = |e| from_zip(zip::result::ZipError::Io(e));

        let dir = self
            .archive_path
            .parent()
            .expect("valid archive is a file, so it has a parent");
        let name = self
            .archive_path
            .file_stem()
            .expect("checked that it is a file, and therefore has a name");
        let zip_path = dir.join(format!(
            "{}.{}.cbz",
            name.to_str().expect("our file paths are utf8 compliant"),
            self.target.ext()
        ));

        let root_dir = Self::get_conversion_root_dir(&self.archive_path);
        let file = File::create(zip_path).map_err(from_io)?;

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        let mut buffer = Vec::new();
        for entry in WalkDir::new(&root_dir).into_iter() {
            let entry =
                entry.map_err(|e| ArchiveError::WalkTempDir(self.archive_path.to_path_buf(), e))?;
            let entry = entry.path();
            let root_parent = root_dir
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

    fn proceed_jobs(&mut self, bar: &ProgressBar) -> Result<(), ImageJobError> {
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

impl Drop for ArchiveJob {
    fn drop(&mut self) {
        // kill all remaining running processes before deleting directory
        self.jobs_in_progress.clear();

        let extract_dir = Self::get_conversion_root_dir(&self.archive_path);
        if extract_dir.exists()
            && let Err(e) = fs::remove_dir_all(&extract_dir)
        {
            error!("error on deleting directory {extract_dir:?}: {e}");
        }
    }
}

pub struct ArchiveJobs(Vec<ArchiveJob>);

#[derive(Error, Debug)]
pub enum ArchiveJobsError {
    #[error("Error while handling an archive")]
    Archive(#[from] ArchiveError),
    #[error("Could not walk the filesystem")]
    ReadingDir(#[from] std::io::Error),
}

impl ArchiveJobs {
    pub fn collect(root_dir: &Path, config: ConversionConfig) -> Result<Self, ArchiveJobsError> {
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
            .collect::<Result<Vec<_>, ArchiveJobsError>>()?;
        Ok(Self(jobs))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn into_iter(self) -> impl IntoIterator<Item = ArchiveJob> {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_for_compressed_jxl() {
        let compressed_path = PathBuf::from("test_data/compressed.jxl");
        assert!(compressed_path.exists());
        let out = ConversionTask::jxl_is_compressed_jpeg(&compressed_path).unwrap();
        assert!(out);
    }

    #[test]
    fn test_check_for_encoded_jxl() {
        let encoded_path = PathBuf::from("test_data/encoded.jxl");
        assert!(encoded_path.exists());
        let out = ConversionTask::jxl_is_compressed_jpeg(&encoded_path).unwrap();
        assert!(!out);
    }
}
