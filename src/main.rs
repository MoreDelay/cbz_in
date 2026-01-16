mod spawn;

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;

use clap::Parser;
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use tracing::{debug, error, info};
use walkdir::WalkDir;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
enum ImageFormat {
    #[default]
    Jpeg,
    Png,
    Avif,
    Jxl,
    Webp,
}

use crate::spawn::{ManagedChild, ProcessError};

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

struct ArchivePath(PathBuf);

impl std::ops::Deref for ArchivePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Error, Debug)]
enum InvalidArchivePath {
    #[error("The provided path does not exist: '{0}'")]
    DoesNotExist(PathBuf),
    #[error("This is not an archive: '{0}'")]
    WrongExtension(PathBuf),
}

impl ArchivePath {
    const ARCHIVE_EXTENSIONS: [&str; 2] = ["zip", "cbz"];

    fn try_new(archive_path: PathBuf) -> Result<Self, InvalidArchivePath> {
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

#[derive(Debug, Error)]
enum TaskError {
    #[error("Conversion not supported from {0:?} to {1:?}")]
    NotSupported(ImageFormat, ImageFormat),
    #[error("Could not query jxl file")]
    Jxlinfo(#[from] spawn::ProcessError),
}

impl ConversionTask {
    fn new(
        image_path: &Path,
        current: ImageFormat,
        target: ImageFormat,
    ) -> Result<Self, TaskError> {
        use ImageFormat::*;

        let out = match (current, target) {
            (a, b) if a == b => return Err(TaskError::NotSupported(current, target)),
            (Avif, Jxl | Webp) => Self::Intermediate(ConversionIntermediate {
                current,
                inbetween: Png,
                target,
            }),
            (Jxl, Avif | Webp) => match jxl_is_compressed_jpeg(image_path)? {
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
        Ok(out)
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
}

#[derive(Debug)]
struct ImageJobWaiting {
    image_path: PathBuf,
    task: ConversionTask,
}
#[derive(Debug)]
struct ImageJobRunning {
    child: ManagedChild,
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

impl ImageJobWaiting {
    fn start_conversion(self) -> Result<ImageJobRunning, ProcessError> {
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

        use ImageFormat::*;
        let child = match (current, target) {
            (Jpeg, Png) => spawn::convert_jpeg_to_png(input_path, output_path)?,
            (Png, Jpeg) => spawn::convert_png_to_jpeg(input_path, output_path)?,
            (Jpeg | Png, Avif) => spawn::encode_avif(input_path, output_path)?,
            (Jpeg | Png, Jxl) => spawn::encode_jxl(input_path, output_path)?,
            (Jpeg | Png, Webp) => spawn::encode_webp(input_path, output_path)?,
            (Avif, Jpeg) => spawn::decode_avif_to_jpeg(input_path, output_path)?,
            (Avif, Png) => spawn::decode_avif_to_png(input_path, output_path)?,
            (Jxl, Jpeg) => spawn::decode_jxl_to_jpeg(input_path, output_path)?,
            (Jxl, Png) => spawn::decode_jxl_to_png(input_path, output_path)?,
            (Webp, Png) => spawn::decode_webp(input_path, output_path)?,
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
    fn child_done(&mut self) -> Result<bool, ConversionError> {
        Ok(self.child.try_wait()?.is_some())
    }
}

impl ImageJobCompleted {
    /// wait on child process and delete original image file
    fn complete(self) -> Result<Option<ImageJobWaiting>, ConversionError> {
        let Self(ImageJobRunning {
            child,
            image_path,
            after,
        }) = self;

        child.wait_with_output()?;
        if let Err(err) = fs::remove_file(&image_path) {
            return Err(ConversionError::DeleteFile(image_path, err));
        };

        let after = after.map(|direct| ImageJobWaiting {
            image_path: image_path.with_extension(direct.current.ext()),
            task: ConversionTask::Direct(direct),
        });
        Ok(after)
    }
}

struct ArchiveJob {
    archive_path: ArchivePath,
    job_queue: VecDeque<ImageJob>,
    jobs_in_progress: Vec<Option<ImageJob>>,
    target: ImageFormat,
}

#[derive(Debug, Error)]
enum NothingToDo {
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

impl ImageJob {
    fn new(image_path: PathBuf, task: ConversionTask) -> Self {
        let waiting = ImageJobWaiting { image_path, task };
        Self::Waiting(waiting)
    }

    fn proceed(self) -> Result<Proceeded, ConversionError> {
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

#[derive(Error, Debug)]
enum ArchiveError {
    #[error("Extract directory already exists at '{0}', delete it and try again")]
    ExtractDirExists(PathBuf),
    #[error("Some IO operation failed")]
    Io(#[from] std::io::Error),
    #[error("An error occurred while extracting")]
    Extracting(#[from] ProcessError),
    #[error("Error while creating conversion tasks")]
    Task(#[from] TaskError),
    #[error("Error during conversion")]
    Conversion(#[from] ConversionError),
    #[error("Error while creating archive")]
    Zipping(#[from] zip::result::ZipError),
    #[error("Got interrupted")]
    Interrupt,
}

#[derive(Error, Debug)]
enum ConversionError {
    #[error("An error occurred in a conversion process")]
    Process(#[from] ProcessError),
    #[error("Could not listen to process signals")]
    Signals(#[source] std::io::Error),
    #[error("Could not delete the file: '{0}'")]
    DeleteFile(PathBuf, #[source] std::io::Error),
}

impl ArchiveJob {
    fn new(
        archive_path: ArchivePath,
        target: ImageFormat,
        n_workers: usize,
        force: bool,
    ) -> Result<Result<Self, NothingToDo>, ArchiveError> {
        if already_converted(&archive_path, target) {
            return Ok(Err(NothingToDo::AlreadyDone(archive_path.into_inner())));
        }

        let extract_dir = get_conversion_root_dir(&archive_path);
        if extract_dir.exists() {
            return Err(ArchiveError::ExtractDirExists(extract_dir));
        }

        let root_dir = get_extraction_root_dir(&archive_path)?;
        let job_queue = images_in_archive(&archive_path)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let image_path = root_dir.join(image_path);
                let task = match ConversionTask::new(&image_path, format, target) {
                    Ok(task) => task,
                    Err(e) => match e {
                        TaskError::NotSupported(..) => {
                            debug!("skip conversion for '{image_path:?}': {e}");
                            return None;
                        }
                        TaskError::Jxlinfo(..) => return Some(Err(e)),
                    },
                };
                let perform = force || task.perform_always();
                perform.then_some(Ok(ImageJob::new(image_path, task)))
            })
            .collect::<Result<VecDeque<_>, _>>()?;
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

    fn extract_cbz(&mut self) -> Result<(), ArchiveError> {
        assert!(self.archive_path.is_file());

        let extract_dir = get_conversion_root_dir(&self.archive_path);
        assert!(!extract_dir.exists());

        fs::create_dir_all(&extract_dir)?;
        spawn::extract_zip(&self.archive_path, &extract_dir)?.wait_with_output()?;
        Ok(())
    }

    fn compress_cbz(&mut self) -> Result<(), ArchiveError> {
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
            name.to_str().unwrap(),
            self.target.ext()
        ));

        let extract_dir = get_conversion_root_dir(&self.archive_path);
        let file = File::create(zip_path)?;

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        let mut buffer = Vec::new();
        for entry in WalkDir::new(&extract_dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let entry = entry.path();
            let file_name = entry.strip_prefix(extract_dir.parent().unwrap()).unwrap();
            let path_string = file_name
                .to_str()
                .to_owned()
                .expect("Path is not UTF-8 conformant");

            if entry.is_file() {
                zipper.start_file(path_string, options)?;
                File::open(entry)?.read_to_end(&mut buffer)?;
                zipper.write_all(&buffer)?;
                buffer.clear();
            } else if !file_name.as_os_str().is_empty() {
                zipper.add_directory(path_string, options)?;
            }
        }

        zipper.finish()?;
        Ok(())
    }

    fn run(mut self) -> Result<(), ArchiveError> {
        assert!(!self.job_queue.is_empty());

        // these signals will be catched from here on out until dropped
        let mut signals = Signals::new([SIGINT, SIGCHLD]).map_err(ConversionError::Signals)?;
        self.extract_cbz()?;

        // start out as many jobs as allowed
        for slot in self.jobs_in_progress.iter_mut() {
            let Some(job) = self.job_queue.pop_front() else {
                break;
            };
            *slot = Some(job);
        }
        self.proceed_jobs()?;

        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => return Err(ArchiveError::Interrupt),
                    SIGCHLD => self.proceed_jobs()?,
                    _ => unreachable!(),
                }
            }
        }
        drop(signals);

        self.compress_cbz()?;
        Ok(())
    }

    fn proceed_jobs(&mut self) -> Result<(), ConversionError> {
        for slot in self.jobs_in_progress.iter_mut() {
            loop {
                match slot.take() {
                    Some(job) => match job.proceed()? {
                        Proceeded::SameAsBefore(job) => {
                            *slot = Some(job);
                            break;
                        }
                        Proceeded::Progress(job) => *slot = Some(job),
                        Proceeded::Finished => (),
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
        let extract_dir = get_conversion_root_dir(&self.archive_path);
        if extract_dir.exists() {
            // ignore errors
            if let Err(e) = fs::remove_dir_all(&extract_dir) {
                error!("error on deleting directory {extract_dir:?}: {e}");
            }
        }
    }
}

fn jxl_is_compressed_jpeg(image_path: &Path) -> Result<bool, TaskError> {
    let has_box = spawn::run_jxlinfo(image_path)?
        .wait_with_output()?
        .stdout
        .lines()
        .any(|line| line.unwrap().starts_with("box: type: \"jbrd\""));
    Ok(has_box)
}

fn images_in_archive(cbz_path: &Path) -> Result<Vec<(PathBuf, ImageFormat)>, ArchiveError> {
    let files = spawn::list_archive_files(cbz_path)?
        .wait_with_output()?
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

fn get_extraction_root_dir(cbz_path: &Path) -> Result<PathBuf, ArchiveError> {
    let child = spawn::list_archive_files(cbz_path)?;

    let archive_name = cbz_path.file_stem().unwrap();
    let archive_root_dirs = child
        .wait_with_output()?
        .stdout
        .lines()
        .filter(|v| v.as_ref().is_ok_and(|line| line.starts_with("Path = ")))
        .map(|v| v.unwrap().strip_prefix("Path = ").unwrap().to_string())
        .filter(|file| !file.contains("/"))
        .collect::<Vec<_>>();

    let has_root_within = archive_root_dirs.len() == 1 && *archive_root_dirs[0] == *archive_name;
    let extract_dir = if has_root_within {
        let parent_dir = cbz_path.parent().unwrap().to_path_buf();
        assert_eq!(
            parent_dir.join(archive_name),
            get_conversion_root_dir(cbz_path)
        );
        parent_dir
    } else {
        get_conversion_root_dir(cbz_path)
    };
    Ok(extract_dir)
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

#[derive(Parser)]
#[command(version, verbatim_doc_comment)]
/// Convert images within comic archives to newer image formats
///
/// Convert images within Zip Comic Book archives, although it also works with normal zip files.
/// By default only converts Jpeg and Png to the target format or decode any formats to Png and
/// Jpeg.
struct Args {
    #[arg(
        required = true,
        help = "All images within the archive(s) are converted to this format"
    )]
    format: ImageFormat,

    #[arg(
        default_value = ".",
        help = "Path to a cbz file or a directory containing cbz files"
    )]
    path: PathBuf,

    /// Number of processes spawned
    ///
    /// Uses as many processes as you have cores by default.
    /// When used as a flag only spawns a single process at a time.
    #[arg(short = 'j', long, verbatim_doc_comment)]
    workers: Option<Option<usize>>,

    #[arg(short, long, help = "Convert all images of all formats")]
    force: bool,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("Invalid archive path provided")]
    InvalidPath(#[from] InvalidArchivePath),
    #[error("Error while handling an archive")]
    Archive(#[from] ArchiveError),
    #[error("Could not walk the filesystem")]
    ReadingDir(#[source] std::io::Error),
}

fn real_main() -> Result<(), AppError> {
    let matches = Args::parse();
    let format = matches.format;
    let path = matches.path;

    let n_workers = match matches.workers {
        Some(Some(value)) => value,
        Some(None) => 1,
        None => match thread::available_parallelism() {
            Ok(value) => value.get(),
            Err(_) => 1,
        },
    };

    let force = matches.force;

    if path.is_dir() {
        for cbz_file in path.read_dir().map_err(AppError::ReadingDir)? {
            let cbz_file = match cbz_file {
                Ok(f) => f,
                Err(e) => {
                    error!("error while walking directory: {e}");
                    continue;
                }
            };
            let cbz_file = cbz_file.path();
            let conversion_file = match ArchivePath::try_new(cbz_file) {
                Ok(f) => f,
                Err(e) => {
                    debug!("skipping: {e}");
                    continue;
                }
            };

            info!("Converting {:?}", conversion_file.0);
            let job = match ArchiveJob::new(conversion_file, format, n_workers, force)? {
                Ok(job) => job,
                Err(e) => {
                    info!("{e}");
                    continue;
                }
            };
            job.run()?;
            info!("Done");
        }
    } else {
        let conversion_file = ArchivePath::try_new(path.clone())?;

        info!("Converting {:?}", conversion_file.0);
        let job = match ArchiveJob::new(conversion_file, format, n_workers, force)? {
            Ok(job) => job,
            Err(e) => {
                error!("{e}");
                return Ok(());
            }
        };
        job.run()?;
        info!("Done");
    }

    Ok(())
}

fn log_error(error: &dyn std::error::Error) {
    error!("{error}");
    let mut source = error.source();
    if source.is_some() {
        error!("Caused by:");
    }
    let mut counter = 0;
    while let Some(error) = source {
        error!("    {counter}: {error}");
        source = error.source();
        counter += 1;
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ret = real_main();
    if let Err(e) = &ret {
        log_error(e);
    }
    Ok(ret?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_for_compressed_jxl() {
        let compressed_path = PathBuf::from("test_data/compressed.jxl");
        assert!(compressed_path.exists());
        let out = jxl_is_compressed_jpeg(&compressed_path).unwrap();
        assert!(out);
    }

    #[test]
    fn test_check_for_encoded_jxl() {
        let encoded_path = PathBuf::from("test_data/encoded.jxl");
        assert!(encoded_path.exists());
        let out = jxl_is_compressed_jpeg(&encoded_path).unwrap();
        assert!(!out);
    }
}
