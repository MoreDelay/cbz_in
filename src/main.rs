use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{exit, Child, Command, Stdio};
use std::thread;

use anyhow::{bail, Result};
use clap::Parser;
use log::{debug, error, trace};
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use walkdir::WalkDir;
use zip::{write::SimpleFileOptions, CompressionMethod, ZipArchive, ZipWriter};

#[derive(Error, Debug)]
enum ConversionError {
    #[error("not an archive")]
    NotArchive,
    #[error("nothing to do for '{0}'")]
    NothingToDo(PathBuf),
    #[error("could not listen to signals")]
    SignalsError,
    #[error("got interrupted")]
    Interrupt,
    #[error("child process finished abnormally for '{0}'")]
    AbnormalExit(PathBuf),
    #[error("could not start process with the program '{0}'")]
    SpawnFailure(String),
    #[error("unspecified error for '{0}'")]
    Unspecific(String),
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default)]
enum ImageFormat {
    #[default]
    Jpeg,
    Png,
    Avif,
    Jxl,
}

#[derive(Default, Clone, Copy, Debug)]
enum JobStatus {
    Init,
    WaitOnProcess,
    #[default]
    Done,
}

#[derive(Default)]
struct ToAvif {
    status: JobStatus,
    image_path: PathBuf,
    #[allow(dead_code)]
    format: ImageFormat,
    child: Option<Child>,
}

#[derive(Default)]
struct ToJxl {
    status: JobStatus,
    image_path: PathBuf,
    #[allow(dead_code)]
    format: ImageFormat,
    child: Option<Child>,
}

enum ConversionJob {
    ToAvif(ToAvif),
    ToJxl(ToJxl),
}

struct WorkUnit {
    cbz_path: PathBuf,
    job_queue: VecDeque<ConversionJob>,
    jobs_in_process: Vec<ConversionJob>,
    target_format: ImageFormat,
    workers: usize,
}

impl ConversionJob {
    fn new(image_path: PathBuf, from: ImageFormat, to: ImageFormat) -> Result<ConversionJob> {
        let job = match to {
            ImageFormat::Avif => ConversionJob::ToAvif(ToAvif {
                status: JobStatus::Init,
                image_path,
                format: from,
                child: None,
            }),
            ImageFormat::Jxl => ConversionJob::ToJxl(ToJxl {
                status: JobStatus::Init,
                image_path,
                format: from,
                child: None,
            }),
            _ => bail!("conversion from {from:?} to {to:?} not supported"),
        };
        Ok(job)
    }

    fn start_conversion_process(
        &mut self,
        root_dir: &PathBuf,
    ) -> Result<JobStatus, ConversionError> {
        match self {
            ConversionJob::ToAvif(job) => {
                let image_path = root_dir.join(&job.image_path);
                debug!("New process working on {:?}", image_path);

                let output_path = image_path.with_extension("avif");
                let mut command = Command::new("cavif");
                command.args([
                    "--speed=3",
                    "--threads=1",
                    "--quality=88",
                    image_path.to_str().unwrap(),
                    "-o",
                    output_path.to_str().unwrap(),
                ]);
                let spawned = command
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|_| ConversionError::SpawnFailure("cavif".to_string()))?;
                job.child = Some(spawned);
                job.status = JobStatus::WaitOnProcess;
                Ok(job.status)
            }
            ConversionJob::ToJxl(job) => {
                let image_path = root_dir.join(&job.image_path);
                debug!("New process working on {:?}", image_path);

                let output_path = image_path.with_extension("jxl");
                let mut command = Command::new("cjxl");
                command.args([
                    "--effort=9",
                    "--num_threads=1",
                    "--distance=0",
                    image_path.to_str().unwrap(),
                    output_path.to_str().unwrap(),
                ]);
                let spawned = command
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|_| ConversionError::SpawnFailure("cjxl".to_string()))?;
                job.child = Some(spawned);
                job.status = JobStatus::WaitOnProcess;
                Ok(job.status)
            }
        }
    }

    // wait on child process and delete original image file
    fn finish_up(&mut self, root_dir: &PathBuf) -> Result<JobStatus, ConversionError> {
        let (status, image_path, child) = match self {
            ConversionJob::ToAvif(job) => (
                &mut job.status,
                job.image_path.clone(),
                &mut job.child.take().unwrap(),
            ),
            ConversionJob::ToJxl(job) => (
                &mut job.status,
                job.image_path.clone(),
                &mut job.child.take().unwrap(),
            ),
        };
        debug!("finish up {image_path:?}");
        let full_image_path = root_dir.join(&image_path);

        match child.wait() {
            Ok(status) if !status.success() => {
                return Err(ConversionError::AbnormalExit(image_path))
            }
            Ok(_) => (),
            Err(_) => return Err(ConversionError::Unspecific("error during wait".to_string())),
        }
        *status = JobStatus::Done;

        match fs::remove_file(full_image_path.clone()) {
            Ok(_) => Ok(status.clone()),
            Err(_) => Err(ConversionError::Unspecific(format!(
                "Could not delete '{full_image_path:?}'"
            ))),
        }
    }

    fn proceed(&mut self, root_dir: &PathBuf) -> Result<JobStatus, ConversionError> {
        let status = match self {
            ConversionJob::ToAvif(job) => job.status,
            ConversionJob::ToJxl(job) => job.status,
        };
        match status {
            JobStatus::Init => self.start_conversion_process(root_dir),
            JobStatus::WaitOnProcess => self.finish_up(root_dir),
            JobStatus::Done => Ok(JobStatus::Done),
        }
    }

    fn can_proceed(&mut self) -> Result<bool, ConversionError> {
        trace!("Called can_proceed() on {self:?}");
        let (status, image_path, child) = match self {
            ConversionJob::ToAvif(job) => (job.status, job.image_path.clone(), &mut job.child),
            ConversionJob::ToJxl(job) => (job.status, job.image_path.clone(), &mut job.child),
        };
        let image_path = image_path.to_owned();

        let child = match child {
            Some(child) => child,
            None => return Ok(false),
        };
        match status {
            JobStatus::Init => unreachable!(),
            JobStatus::WaitOnProcess => (),
            JobStatus::Done => return Ok(false),
        }

        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(true),
            Ok(_) => return Ok(false),
            Err(_) => {
                return Err(ConversionError::Unspecific(
                    image_path.to_string_lossy().to_string(),
                ))
            }
        }
    }
}

impl WorkUnit {
    fn new(cbz_path: PathBuf, target_format: ImageFormat, workers: usize) -> Result<WorkUnit> {
        let job_queue = images_in_archive(&cbz_path)?
            .iter()
            .filter_map(|(image_path, format)| {
                ConversionJob::new(image_path.clone(), *format, target_format).ok()
            })
            .collect();

        Ok(WorkUnit {
            cbz_path,
            job_queue,
            jobs_in_process: vec![],
            target_format,
            workers,
        })
    }

    fn run(mut self) -> Result<(), ConversionError> {
        debug!("Start conversion for {:?}", self.cbz_path);
        if self.job_queue.is_empty() {
            return Err(ConversionError::NothingToDo(self.cbz_path.clone()));
        }

        let extract_dir = extract_cbz(&self.cbz_path);

        let mut signals = match Signals::new(&[SIGINT, SIGCHLD]) {
            Ok(signals) => signals,
            Err(_) => return Err(ConversionError::SignalsError),
        };

        // start out as many jobs as allowed
        while self.jobs_in_process.len() < self.workers {
            let mut job = match self.job_queue.pop_front() {
                Some(job) => job,
                None => break,
            };

            let status = job.proceed(&extract_dir)?;
            match status {
                JobStatus::WaitOnProcess => {
                    self.jobs_in_process.push(job);
                }
                JobStatus::Done => (),
                JobStatus::Init => unreachable!(),
            }
        }

        // add new jobs as other jobs complete
        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => {
                        debug!("Got signal SIGINT");
                        return Err(ConversionError::Interrupt);
                    }
                    SIGCHLD => {
                        debug!("Got signal SIGCHLD");
                        self.proceed_jobs(&extract_dir)?;
                        if !self.job_queue.is_empty() {
                            self.start_next_jobs(&extract_dir)?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        compress_cbz(&self.cbz_path, self.target_format);
        Ok(())
    }

    fn proceed_jobs(&mut self, root_dir: &PathBuf) -> Result<(), ConversionError> {
        trace!("proceed all ready jobs");
        for job in self.jobs_in_process.iter_mut() {
            trace!("job in process: {job:?}");
            if job.can_proceed()? {
                debug!("proceed with {job:?}");
                match job.proceed(root_dir)? {
                    JobStatus::Done => (),
                    JobStatus::Init | JobStatus::WaitOnProcess => unreachable!(),
                }
            }
        }
        Ok(())
    }

    fn start_next_jobs(&mut self, extract_dir: &PathBuf) -> Result<(), ConversionError> {
        trace!("start new jobs");
        for job in self.jobs_in_process.iter_mut() {
            trace!("job in process: {job:?}");
            let status = match job {
                ConversionJob::ToAvif(job) => job.status,
                ConversionJob::ToJxl(job) => job.status,
            };
            if let JobStatus::Done = status {
                let mut new_job = self.job_queue.pop_front().unwrap();
                debug!("replace job {job:?} for {new_job:?}");
                std::mem::swap(job, &mut new_job);
                job.proceed(extract_dir)?;
            }
        }
        Ok(())
    }

    fn jobs_pending(&self) -> bool {
        let all_done = self
            .jobs_in_process
            .iter()
            .map(|job| match job {
                ConversionJob::ToAvif(job) => job.status,
                ConversionJob::ToJxl(job) => job.status,
            })
            .all(|status| {
                if let JobStatus::Done = status {
                    true
                } else {
                    false
                }
            });
        !all_done
    }
}

impl std::fmt::Debug for ConversionJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionJob::ToAvif(job) => f
                .debug_struct("ToAvif")
                .field("image_path", &job.image_path.to_string_lossy())
                .field("status", &job.status)
                .finish(),
            ConversionJob::ToJxl(job) => f
                .debug_struct("ToJxl")
                .field("image_path", &job.image_path.to_string_lossy())
                .field("status", &job.status)
                .finish(),
        }
    }
}

impl Drop for ConversionJob {
    fn drop(&mut self) {
        trace!("Drop {self:?}");
        let child = match self {
            ConversionJob::ToAvif(job) => &mut job.child.take(),
            ConversionJob::ToJxl(job) => &mut job.child.take(),
        };
        let child: &mut Child = match child {
            Some(ref mut child) => child,
            None => return,
        };

        let _ = child.kill();
        let _ = child.wait(); // is this necessary?
    }
}

impl Drop for WorkUnit {
    fn drop(&mut self) {
        debug!("Cleanup for {:?}", self.cbz_path);
        let extract_dir = extract_dir_from_cbz_path(&self.cbz_path);
        if extract_dir.exists() {
            fs::remove_dir_all(extract_dir.clone()).unwrap();
        }
    }
}

fn images_in_archive(cbz_path: &Path) -> Result<Vec<(PathBuf, ImageFormat)>> {
    trace!("Called cbz_contains_convertable_images()");

    if let None = cbz_path.extension() {
        bail!("No extension");
    }
    if cbz_path.extension().map_or(false, |e| e != "cbz") {
        bail!("Wrong extension");
    }

    let file = File::open(cbz_path).unwrap();
    let reader = BufReader::new(file);

    let archive = ZipArchive::new(reader).unwrap();
    let mut result = vec![];
    for file_inside in archive.file_names() {
        let file_inside = PathBuf::from(file_inside);
        trace!("Looking at file: {:?}", file_inside);
        if let Some(ext) = file_inside.extension() {
            match ext.to_str().unwrap() {
                "jpg" => result.push((file_inside, ImageFormat::Jpeg)),
                "jpeg" => result.push((file_inside, ImageFormat::Jpeg)),
                "png" => result.push((file_inside, ImageFormat::Png)),
                _ => (),
            }
        }
    }
    Ok(result)
}

fn extract_dir_from_cbz_path(path: &Path) -> PathBuf {
    let dir = path.parent().unwrap();
    let name = path.file_stem().unwrap();
    let extract_dir = dir.join(name);
    extract_dir
}

fn already_converted(path: &PathBuf, format: ImageFormat) -> bool {
    let conversion_ending = match format {
        ImageFormat::Avif => ".avif.cbz",
        ImageFormat::Jxl => ".jxl.cbz",
        ImageFormat::Jpeg => todo!(),
        ImageFormat::Png => todo!(),
    };

    let dir = path.parent().unwrap();
    let name = path.file_stem().unwrap();
    let zip_path = dir.join(format!("{}{conversion_ending}", name.to_str().unwrap()));

    let is_converted_archive = path.to_str().unwrap().ends_with(conversion_ending);
    let has_converted_archive = zip_path.exists();

    trace!(" is converted archive? {is_converted_archive}");
    trace!("has converted archive? {has_converted_archive}");
    is_converted_archive || has_converted_archive
}

fn has_root_within_archive(cbz_path: &PathBuf) -> bool {
    let file = File::open(cbz_path).unwrap();
    let reader = BufReader::new(file);

    let archive = ZipArchive::new(reader).unwrap();
    let root_dirs: Vec<_> = archive
        .file_names()
        .into_iter()
        .filter(|s| s.ends_with("/"))
        .filter(|s| s.find("/").unwrap() == s.len() - 1)
        .collect();
    root_dirs.len() == 1 && root_dirs[0].strip_suffix("/").unwrap() == cbz_path.file_stem().unwrap()
}

fn extract_cbz(cbz_path: &PathBuf) -> PathBuf {
    trace!("Called extract_cbz() with {:?}", cbz_path);
    assert!(cbz_path.is_file());

    let extract_dir = if has_root_within_archive(cbz_path) {
        trace!("extract directly");
        cbz_path.parent().unwrap().to_path_buf()
    } else {
        trace!("extract into new root directory");
        extract_dir_from_cbz_path(cbz_path)
    };
    let file = File::open(cbz_path).unwrap();
    let reader = BufReader::new(file);
    let mut archive = ZipArchive::new(reader).unwrap();

    debug!("Extracting {:?} to {:?}", cbz_path, extract_dir);
    fs::create_dir_all(extract_dir.clone()).unwrap();
    archive.extract(extract_dir.clone()).unwrap();
    extract_dir
}

fn compress_cbz(cbz_path: &PathBuf, target_format: ImageFormat) {
    trace!("Called compress_cbz() with {:?}", cbz_path);

    let dir = cbz_path.parent().unwrap();
    let name = cbz_path.file_stem().unwrap();
    let zip_path = match target_format {
        ImageFormat::Avif => dir.join(format!("{}.avif.cbz", name.to_str().unwrap())),
        ImageFormat::Jxl => dir.join(format!("{}.jxl.cbz", name.to_str().unwrap())),
        ImageFormat::Jpeg => panic!("not supported target {target_format:?}"),
        ImageFormat::Png => panic!("not supported target {target_format:?}"),
    };
    debug!("Create cbz at {:?}", zip_path);
    let file = File::create(zip_path).unwrap();

    let mut zipper = ZipWriter::new(file);
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o755);

    let extract_dir = extract_dir_from_cbz_path(cbz_path);
    let mut buffer = Vec::new();
    for entry in WalkDir::new(&extract_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let entry = entry.path();
        debug!("Add to archive: {:?}", entry);
        let file_name = entry.strip_prefix(&extract_dir.parent().unwrap()).unwrap();
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
}

fn convert_single_cbz(
    cbz_file: &PathBuf,
    format: ImageFormat,
    workers: usize,
) -> Result<(), ConversionError> {
    trace!("Called convert_single_cbz() with {:?}", cbz_file);
    if already_converted(&cbz_file, format) {
        println!("Conversion already done for {:?}", cbz_file);
        return Ok(());
    }

    let work_unit = match WorkUnit::new(cbz_file.clone(), format, workers) {
        Ok(work_unit) => work_unit,
        Err(_) => {
            return Err(ConversionError::NotArchive);
        }
    };

    println!("Converting {:?}", cbz_file);
    match work_unit.run() {
        Ok(_) => println!("Done"),
        Err(ConversionError::NothingToDo(_)) => println!("Nothing to do"),
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

#[derive(Parser)]
#[command(version, about, long_about=None)]
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

    #[arg(
        short = 'j',
        long,
        help = "Number of processes spawned to convert images in parallel"
    )]
    workers: Option<usize>,
}

fn main() -> Result<()> {
    pretty_env_logger::init();

    let matches = Args::parse();
    let format = matches.format;
    let path = matches.path;
    if !path.exists() {
        error!("Does not exists: {:?}", path);
        exit(1);
    }

    let workers = matches
        .workers
        .unwrap_or_else(|| match thread::available_parallelism() {
            Ok(value) => value.get(),
            Err(_) => 1,
        });

    if path.is_dir() {
        for cbz_file in path.read_dir().expect("read dir call failed!") {
            if let Ok(cbz_file) = cbz_file {
                let cbz_file = cbz_file.path();
                debug!("Next path: {:?}", cbz_file);
                if let Err(e) = convert_single_cbz(&cbz_file, format, workers) {
                    debug!("{e:?}");
                }
            }
        }
    } else {
        if let Err(e) = convert_single_cbz(&path, format, workers) {
            match e {
                ConversionError::NothingToDo(_) => println!("Nothing to do for {path:?}"),
                ConversionError::NotArchive => println!("This is not a Zip archive"),
                _ => error!("{e:?}"),
            }
        }
    }
    Ok(())
}
