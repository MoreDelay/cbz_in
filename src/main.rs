mod spawn;

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::thread;

use clap::Parser;
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use tracing::{debug, error, info, trace};
use walkdir::WalkDir;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

#[derive(Error, Debug)]
enum ConversionError {
    #[error("No archive at '{0}'")]
    DoesNotExist(PathBuf),
    #[error("Not an archive '{0}'")]
    NotAnArchive(PathBuf),
    #[error("Conversion not supported from {0:?} to {1:?}")]
    NotSupported(ImageFormat, ImageFormat),
    #[error("Got interrupted")]
    Interrupt,
    #[error("Error during extraction: {0}")]
    ExtractionError(String),
    #[error("Child process finished abnormally for '{0}'")]
    AbnormalExit(PathBuf),
    #[error("Could not start process")]
    ProcessSpawn(#[from] spawn::SpawnError),
    #[error("Unspecific error '{0}'")]
    Unspecific(String, #[source] Box<dyn std::error::Error + Send + Sync>),
}
use ConversionError::*;

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq)]
enum ImageFormat {
    #[default]
    Jpeg,
    Png,
    Avif,
    Jxl,
    Webp,
}
use ImageFormat::*;

use crate::spawn::ManagedChild;

impl ImageFormat {
    fn ext(self) -> &'static str {
        match self {
            Jpeg => "jpeg",
            Png => "png",
            Avif => "avif",
            Jxl => "jxl",
            Webp => "webp",
        }
    }
}

struct ValidArchive {
    archive_path: PathBuf,
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

impl ConversionTask {
    fn new(
        image_path: &Path,
        current: ImageFormat,
        target: ImageFormat,
    ) -> Result<Self, ConversionError> {
        let out = match (current, target) {
            (a, b) if a == b => return Err(NotSupported(current, target)),
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

    fn conversion_tuple(&self) -> (ImageFormat, ImageFormat) {
        match self {
            ConversionTask::Direct(ConversionDirect { current, target }) => (*current, *target),
            ConversionTask::Intermediate(ConversionIntermediate {
                current, target, ..
            }) => (*current, *target),
        }
    }

    fn perform_always(&self) -> bool {
        match self.conversion_tuple() {
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
enum ImageJob {
    Waiting(ImageJobWaiting),
    Running(ImageJobRunning),
}

impl ImageJobWaiting {
    fn start_conversion(self) -> Result<ImageJobRunning, ConversionError> {
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
    /// wait on child process and delete original image file
    fn complete(self) -> Result<Option<ImageJobWaiting>, ConversionError> {
        let ImageJobRunning {
            mut child,
            image_path,
            after,
        } = self;

        match child.wait() {
            Ok(status) if !status.success() => {
                let output = extract_console_output(&mut child);
                debug!("error on process {image_path:?}:\n{output}");
                return Err(AbnormalExit(image_path));
            }
            Ok(_) => {
                let output = extract_console_output(&mut child);
                trace!("process output:\n{output}");
            }
            Err(e) => return Err(Unspecific("error during wait".to_string(), e.into())),
        }

        fs::remove_file(&image_path).map_err(|e| {
            Unspecific(
                format!("completing conversion: Could not delete '{:?}'", image_path),
                e.into(),
            )
        })?;

        let after = after.map(|direct| ImageJobWaiting {
            image_path: image_path.with_extension(direct.current.ext()),
            task: ConversionTask::Direct(direct),
        });
        Ok(after)
    }
}

struct ArchiveJob {
    archive_path: PathBuf,
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

impl ValidArchive {
    fn try_new(archive_path: PathBuf) -> Result<Self, ConversionError> {
        const EXPECTED_EXTENSION: [&str; 2] = ["zip", "cbz"];

        let correct_extension = archive_path.extension().is_some_and(|ext| {
            EXPECTED_EXTENSION
                .iter()
                .any(|valid_ext| ext.eq_ignore_ascii_case(valid_ext))
        });
        if !correct_extension {
            return Err(NotAnArchive(archive_path));
        }
        Ok(ValidArchive { archive_path })
    }
}

impl ImageJob {
    fn new(image_path: PathBuf, task: ConversionTask) -> Self {
        let waiting = ImageJobWaiting { image_path, task };
        Self::Waiting(waiting)
    }

    fn proceed(self) -> Result<Option<Self>, ConversionError> {
        debug!("proceed with {self:?}");
        let running = match self {
            ImageJob::Waiting(waiting) => {
                let running = waiting.start_conversion()?;
                Some(Self::Running(running))
            }
            ImageJob::Running(running) => running
                .complete()?
                .map(|waiting| waiting.start_conversion())
                .transpose()?
                .map(Self::Running),
        };
        debug!("after proceed {running:?}");
        Ok(running)
    }

    fn can_proceed(&mut self) -> Result<bool, ConversionError> {
        let (child, image_path) = match self {
            ImageJob::Waiting(_) => return Ok(true),
            ImageJob::Running(running) => (&mut running.child, &running.image_path),
        };

        match child.try_wait() {
            Ok(Some(_)) => {
                trace!("ready");
                Ok(true)
            }
            Ok(None) => {
                trace!("not ready");
                Ok(false)
            }
            Err(e) => {
                trace!("error");
                Err(Unspecific(
                    image_path.to_string_lossy().to_string(),
                    e.into(),
                ))
            }
        }
    }
}

impl ArchiveJob {
    fn with(
        conversion_file: ValidArchive,
        target: ImageFormat,
        n_workers: usize,
        force: bool,
    ) -> Result<Result<Self, NothingToDo>, ConversionError> {
        trace!("called WorkUnit::new()");
        let ValidArchive { archive_path } = conversion_file;

        if already_converted(&archive_path, target) {
            return Ok(Err(NothingToDo::AlreadyDone(archive_path.to_path_buf())));
        }

        let extract_dir = get_conversion_root_dir(&archive_path);
        if extract_dir.exists() {
            return Err(ConversionError::ExtractionError(
                "Extract directory already exists, delete it and try again".to_string(),
            ));
        }

        let root_dir = get_extraction_root_dir(&archive_path)?;
        let job_queue = images_in_archive(&archive_path)?
            .into_iter()
            .filter_map(|(image_path, format)| {
                let image_path = root_dir.join(image_path);
                let task = ConversionTask::new(&image_path, format, target).ok()?;
                let perform = force || task.perform_always();
                perform.then_some(ImageJob::new(image_path, task))
            })
            .collect::<VecDeque<_>>();
        if job_queue.is_empty() {
            return Ok(Err(NothingToDo::NoFilesToConvert(archive_path)));
        }

        let jobs_in_progress = Vec::from_iter((0..n_workers).map(|_| None));
        Ok(Ok(Self {
            archive_path,
            job_queue,
            jobs_in_progress,
            target,
        }))
    }

    fn extract_cbz(&mut self) -> Result<(), ConversionError> {
        trace!("called extract_cbz() with {:?}", self.archive_path);
        assert!(self.archive_path.is_file());

        let extract_dir = get_conversion_root_dir(&self.archive_path);
        assert!(!extract_dir.exists());

        debug!("extracting {:?} to {:?}", self.archive_path, extract_dir);
        fs::create_dir_all(&extract_dir).unwrap();

        let mut signals = Signals::new([SIGINT, SIGCHLD])
            .map_err(|e| Unspecific("could not listen to signals".to_string(), e.into()))?;

        let child = spawn::extract_zip(&self.archive_path, &extract_dir)?;

        #[expect(
            clippy::never_loop,
            reason = "signal-hook says it can spuriously return without any signals set"
        )]
        'outer: loop {
            for signal in signals.wait() {
                match signal {
                    SIGINT => {
                        debug!("interrupted while extracting");
                        return Err(Interrupt);
                    }
                    SIGCHLD => {
                        break 'outer;
                    }
                    _ => unreachable!(),
                }
            }
        }
        debug!("extraction done");

        match child.into_inner().wait_with_output() {
            Ok(output) if output.status.code().is_some_and(|code| code == 0) => Ok(()),
            Ok(_) => Err(ConversionError::ExtractionError(
                "Extraction with 7z unsuccessful".to_string(),
            )),
            Err(e) => Err(ConversionError::ExtractionError(e.to_string())),
        }
    }

    fn compress_cbz(&mut self) {
        trace!("called compress_cbz() with {:?}", self.archive_path);

        let dir = self.archive_path.parent().unwrap();
        let name = self.archive_path.file_stem().unwrap();
        let zip_path = dir.join(format!(
            "{}.{}.cbz",
            name.to_str().unwrap(),
            self.target.ext()
        ));

        let extract_dir = get_conversion_root_dir(&self.archive_path);
        debug!("compress dir {extract_dir:?} to archive {zip_path:?}");
        let file = File::create(zip_path).unwrap();

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
            trace!("add to archive: {:?}", entry);
            let file_name = entry.strip_prefix(extract_dir.parent().unwrap()).unwrap();
            let path_string = file_name
                .to_str()
                .to_owned()
                .expect("Path is not UTF-8 conformant");

            if entry.is_file() {
                zipper.start_file(path_string, options).unwrap();
                File::open(entry).unwrap().read_to_end(&mut buffer).unwrap();
                zipper.write_all(&buffer).unwrap();
                buffer.clear();
            } else if !file_name.as_os_str().is_empty() {
                zipper.add_directory(path_string, options).unwrap();
            }
        }

        zipper.finish().unwrap();
        debug!("compression done");
    }

    fn run(mut self) -> Result<(), ConversionError> {
        debug!("start conversion for {:?}", self.archive_path);

        assert!(!self.job_queue.is_empty());
        self.extract_cbz()?;

        // these signals will be catched from here on out until dropped
        let mut signals = Signals::new([SIGINT, SIGCHLD])
            .map_err(|e| Unspecific("could not listen to signals".to_string(), e.into()))?;

        // start out as many jobs as allowed
        trace!("start initial jobs");
        for slot in self.jobs_in_progress.iter_mut() {
            let Some(job) = self.job_queue.pop_front() else {
                break;
            };
            if let Some(job) = job.proceed()? {
                *slot = Some(job);
            }
        }

        trace!("start new jobs as old ones complete");
        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => {
                        debug!("got signal SIGINT while converting");
                        return Err(Interrupt);
                    }
                    SIGCHLD => {
                        debug!("got signal SIGCHLD for conversion");
                        self.proceed_jobs()?;
                        if !self.job_queue.is_empty() {
                            self.start_next_jobs()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
        drop(signals);
        debug!("done converting");

        self.compress_cbz();
        Ok(())
    }

    fn proceed_jobs(&mut self) -> Result<(), ConversionError> {
        trace!("proceed all ready jobs");
        for slot in self.jobs_in_progress.iter_mut() {
            trace!("job in progress: {slot:?}");
            let Some(mut job) = slot.take() else { continue };

            let job = match job.can_proceed()? {
                true => job.proceed()?,
                false => Some(job),
            };
            *slot = job;
        }
        Ok(())
    }

    fn start_next_jobs(&mut self) -> Result<(), ConversionError> {
        trace!("start new jobs");
        'replace: for slot in self.jobs_in_progress.iter_mut() {
            if slot.is_some() {
                continue;
            }
            let new_job = 'search: loop {
                let new_job = match self.job_queue.pop_front() {
                    Some(new_job) => new_job,
                    None => break 'replace,
                };
                match new_job.proceed()? {
                    Some(new_job) => break 'search new_job,
                    None => continue,
                }
            };
            *slot = Some(new_job);
        }
        Ok(())
    }

    fn jobs_pending(&self) -> bool {
        self.jobs_in_progress.iter().any(|job| job.is_some())
    }
}

impl Drop for ArchiveJob {
    fn drop(&mut self) {
        debug!("cleanup for {:?}", self.archive_path);
        let extract_dir = get_conversion_root_dir(&self.archive_path);
        if extract_dir.exists() {
            debug!("delete directory {extract_dir:?}");
            // ignore errors
            let _ = fs::remove_dir_all(&extract_dir);
        }
    }
}

fn extract_console_output(child: &mut Child) -> String {
    let stdout = child.stdout.as_mut().unwrap();
    let mut output = String::new();
    stdout.read_to_string(&mut output).unwrap();
    let stderr = child.stderr.as_mut().unwrap();
    let mut err_out = String::new();
    stderr.read_to_string(&mut err_out).unwrap();
    format!("stdout:\n{output}\nstderr:\n{err_out}")
}

fn jxl_is_compressed_jpeg(image_path: &Path) -> Result<bool, ConversionError> {
    let mut child = spawn::run_jxlinfo(image_path)?;

    match child.wait() {
        Ok(status) if !status.success() => {
            let output = extract_console_output(&mut child);
            debug!("error on process:\n{output}");
            Err(AbnormalExit(image_path.to_path_buf()))
        }
        Ok(_) => {
            let output = extract_console_output(&mut child);
            trace!("process output:\n{output}");

            let has_jbrd_box = output
                .lines()
                .any(|line| line.starts_with("box: type: \"jbrd\""));
            Ok(has_jbrd_box)
        }
        Err(e) => Err(Unspecific("error during wait".to_string(), e.into())),
    }
}

fn images_in_archive(cbz_path: &Path) -> Result<Vec<(PathBuf, ImageFormat)>, ConversionError> {
    trace!("called images_in_archive()");

    let child = spawn::list_archive_files(cbz_path)?;
    let files = child
        .into_inner()
        .wait_with_output()
        .map_err(|e| {
            ConversionError::Unspecific("Could not wait on 7z process".to_string(), e.into())
        })?
        .stdout
        .lines()
        .filter_map(|v| match v {
            Ok(line) => line.strip_prefix("Path = ").map(PathBuf::from),
            Err(_) => None,
        })
        .filter_map(|file| {
            trace!("found file {file:?}");
            let extension = file.extension()?.to_string_lossy().to_lowercase();
            match extension.as_str() {
                "jpg" => Some((file, Jpeg)),
                "jpeg" => Some((file, Jpeg)),
                "png" => Some((file, Png)),
                "avif" => Some((file, Avif)),
                "jxl" => Some((file, Jxl)),
                "webp" => Some((file, Webp)),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    Ok(files)
}

fn get_extraction_root_dir(cbz_path: &Path) -> Result<PathBuf, ConversionError> {
    let child = spawn::list_archive_files(cbz_path)?;

    let archive_name = cbz_path.file_stem().unwrap();
    let archive_root_dirs = child
        .into_inner()
        .wait_with_output()
        .map_err(|e| {
            ConversionError::Unspecific("Could not wait on 7z process".to_string(), e.into())
        })?
        .stdout
        .lines()
        .filter(|v| v.as_ref().is_ok_and(|line| line.starts_with("Path = ")))
        .map(|v| v.unwrap().strip_prefix("Path = ").unwrap().to_string())
        .filter(|file| !file.contains("/"))
        .collect::<Vec<_>>();

    let has_root_within = archive_root_dirs.len() == 1 && *archive_root_dirs[0] == *archive_name;
    let extract_dir = if has_root_within {
        trace!("extract directly");
        let parent_dir = cbz_path.parent().unwrap().to_path_buf();
        assert_eq!(
            parent_dir.join(archive_name),
            get_conversion_root_dir(cbz_path)
        );
        parent_dir
    } else {
        trace!("extract into new root directory");
        get_conversion_root_dir(cbz_path)
    };
    Ok(extract_dir)
}

fn get_conversion_root_dir(cbz_path: &Path) -> PathBuf {
    let dir = cbz_path.parent().unwrap();
    let name = cbz_path.file_stem().unwrap();
    dir.join(name)
}

fn already_converted(path: &Path, format: ImageFormat) -> bool {
    let conversion_ending = format!(".{}.cbz", format.ext());

    let dir = path.parent().unwrap();
    let name = path.file_stem().unwrap();
    let zip_path = dir.join(format!("{}{}", name.to_str().unwrap(), conversion_ending));

    let is_converted_archive = path.to_str().unwrap().ends_with(&conversion_ending);
    let has_converted_archive = zip_path.exists();

    trace!(" is converted archive? {is_converted_archive}");
    trace!("has converted archive? {has_converted_archive}");
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

fn real_main() -> Result<(), ConversionError> {
    let matches = Args::parse();
    let format = matches.format;
    let path = matches.path;
    if !path.exists() {
        return Err(ConversionError::DoesNotExist(path));
    }

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
        for cbz_file in path.read_dir().expect("could not read dir").flatten() {
            let cbz_file = cbz_file.path();
            let Ok(conversion_file) = ValidArchive::try_new(cbz_file) else {
                continue;
            };

            info!("Converting {:?}", conversion_file.archive_path);

            let job = match ArchiveJob::with(conversion_file, format, n_workers, force)? {
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
        let conversion_file = ValidArchive::try_new(path.clone())?;

        info!("Converting {:?}", conversion_file.archive_path);

        let job = match ArchiveJob::with(conversion_file, format, n_workers, force)? {
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
