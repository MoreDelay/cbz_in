use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{exit, Child, Command, Stdio};
use std::thread;

use anyhow::Result;
use clap::Parser;
use log::{debug, error, info, trace, warn};
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use walkdir::WalkDir;
use zip::{write::SimpleFileOptions, CompressionMethod, ZipArchive, ZipWriter};

#[derive(Error, Debug)]
enum ConversionError {
    #[error("not an archive '{0}'")]
    NotAnArchive(PathBuf),
    #[error("nothing to do for '{0}'")]
    NothingToDo(PathBuf),
    #[error("conversion not supported from {0:?} to {1:?}")]
    NotSupported(ImageFormat, ImageFormat),
    #[error("Conversion already done for '{0}'")]
    AlreadyDone(PathBuf),
    #[error("got interrupted")]
    Interrupt,
    #[error("child process finished abnormally for '{0}'")]
    AbnormalExit(PathBuf),
    #[error("could not start process with the program '{0}'")]
    SpawnFailure(String),
    #[error("unspecific error '{0}'")]
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
    Converting,
    #[default]
    Done,
}

struct ConversionJob {
    status: JobStatus,
    image_path: PathBuf,
    current: ImageFormat,
    target: ImageFormat,
    child: Option<Child>,
}

struct WorkUnit {
    cbz_path: PathBuf,
    job_queue: VecDeque<ConversionJob>,
    jobs_in_process: Vec<ConversionJob>,
    target_format: ImageFormat,
    workers: usize,
}

impl ConversionJob {
    fn new(
        image_path: PathBuf,
        from: ImageFormat,
        to: ImageFormat,
    ) -> Result<ConversionJob, ConversionError> {
        let result = match (from, to) {
            (ImageFormat::Jpeg | ImageFormat::Png, ImageFormat::Avif) => Ok(()),
            (ImageFormat::Jpeg | ImageFormat::Png, ImageFormat::Jxl) => Ok(()),
            (ImageFormat::Jpeg, ImageFormat::Jpeg) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Jpeg, ImageFormat::Png) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Png, ImageFormat::Jpeg) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Png, ImageFormat::Png) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Avif, ImageFormat::Jpeg) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Avif, ImageFormat::Png) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Avif, ImageFormat::Avif) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Avif, ImageFormat::Jxl) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Jxl, ImageFormat::Jpeg) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Jxl, ImageFormat::Png) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Jxl, ImageFormat::Avif) => Err(ConversionError::NotSupported(from, to)),
            (ImageFormat::Jxl, ImageFormat::Jxl) => Err(ConversionError::NotSupported(from, to)),
        };
        if let Err(e) = result {
            warn!("{e}");
            return Err(e);
        }

        Ok(ConversionJob {
            status: JobStatus::Init,
            image_path,
            current: from,
            target: to,
            child: None,
        })
    }

    fn start_conversion_process(&mut self) -> Result<JobStatus, ConversionError> {
        let next_status = match (self.current, self.target) {
            (ImageFormat::Jpeg | ImageFormat::Png, ImageFormat::Avif) => {
                let output_path = self.image_path.with_extension("avif");
                let mut command = Command::new("cavif");
                command.args([
                    "--speed=3",
                    "--threads=1",
                    "--quality=88",
                    self.image_path.to_str().unwrap(),
                    "-o",
                    output_path.to_str().unwrap(),
                ]);
                let spawned = command
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|_| ConversionError::SpawnFailure("cavif".to_string()))?;
                self.child = Some(spawned);
                JobStatus::Converting
            }
            (ImageFormat::Jpeg | ImageFormat::Png, ImageFormat::Jxl) => {
                let output_path = self.image_path.with_extension("jxl");
                let mut command = Command::new("cjxl");
                command.args([
                    "--effort=9",
                    "--num_threads=1",
                    "--distance=0",
                    self.image_path.to_str().unwrap(),
                    output_path.to_str().unwrap(),
                ]);
                let spawned = command
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|_| ConversionError::SpawnFailure("cjxl".to_string()))?;
                self.child = Some(spawned);
                JobStatus::Converting
            }
            (ImageFormat::Jpeg, ImageFormat::Jpeg) => JobStatus::Done,
            (ImageFormat::Jpeg, ImageFormat::Png) => todo!(),
            (ImageFormat::Png, ImageFormat::Jpeg) => todo!(),
            (ImageFormat::Png, ImageFormat::Png) => JobStatus::Done,
            (ImageFormat::Avif, ImageFormat::Jpeg) => todo!(),
            (ImageFormat::Avif, ImageFormat::Png) => todo!(),
            (ImageFormat::Avif, ImageFormat::Avif) => JobStatus::Done,
            (ImageFormat::Avif, ImageFormat::Jxl) => todo!(),
            (ImageFormat::Jxl, ImageFormat::Jpeg) => todo!(),
            (ImageFormat::Jxl, ImageFormat::Png) => todo!(),
            (ImageFormat::Jxl, ImageFormat::Avif) => todo!(),
            (ImageFormat::Jxl, ImageFormat::Jxl) => JobStatus::Done,
        };
        self.status = next_status;
        Ok(next_status)
    }

    // wait on child process and delete original image file
    fn finish_up(&mut self) -> Result<JobStatus, ConversionError> {
        let child: &mut Child = match &mut self.child {
            Some(child) => child,
            None => unreachable!(),
        };
        match child.wait() {
            Ok(status) if !status.success() => {
                return Err(ConversionError::AbnormalExit(self.image_path.clone()))
            }
            Ok(_) => (),
            Err(_) => return Err(ConversionError::Unspecific("error during wait".to_string())),
        }

        self.status = JobStatus::Done;
        match fs::remove_file(self.image_path.clone()) {
            Ok(_) => Ok(self.status),
            Err(_) => Err(ConversionError::Unspecific(format!(
                "Could not delete '{:?}'",
                self.image_path
            ))),
        }
    }

    fn proceed(&mut self) -> Result<JobStatus, ConversionError> {
        debug!("proceed with {self:?}");
        let result = match self.status {
            JobStatus::Init => self.start_conversion_process(),
            JobStatus::Converting => self.finish_up(),
            JobStatus::Done => Ok(JobStatus::Done),
        };
        debug!("after proceed {self:?}");
        result
    }

    fn can_proceed(&mut self) -> Result<bool, ConversionError> {
        match self.status {
            JobStatus::Init => unreachable!(),
            JobStatus::Converting => (),
            JobStatus::Done => return Ok(false),
        }
        let child: &mut Child = match &mut self.child {
            Some(child) => child,
            None => unreachable!(),
        };
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                trace!("ready");
                return Ok(true);
            }
            Ok(_) => {
                trace!("not ready");
                return Ok(false);
            }
            Err(_) => {
                trace!("error");
                return Err(ConversionError::Unspecific(
                    self.image_path.to_string_lossy().to_string(),
                ));
            }
        }
    }
}

impl WorkUnit {
    fn new(
        cbz_path: PathBuf,
        target_format: ImageFormat,
        workers: usize,
    ) -> Result<WorkUnit, ConversionError> {
        trace!("called WorkUnit::new()");
        let not_correct_extention = cbz_path
            .extension()
            .map_or(true, |e| e != "cbz" && e != "zip");
        if not_correct_extention {
            return Err(ConversionError::NotAnArchive(cbz_path.to_path_buf()));
        }

        let root_dir = get_extraction_root_dir(&cbz_path);
        let job_queue = images_in_archive(&cbz_path)?
            .iter()
            .filter_map(|(image_path, format)| {
                ConversionJob::new(root_dir.join(image_path), *format, target_format).ok()
            })
            .collect::<VecDeque<_>>();
        if job_queue.is_empty() {
            return Err(ConversionError::NothingToDo(cbz_path));
        }

        Ok(WorkUnit {
            cbz_path,
            job_queue,
            jobs_in_process: vec![],
            target_format,
            workers,
        })
    }

    fn extract_cbz(&mut self) {
        trace!("called extract_cbz() with {:?}", self.cbz_path);
        assert!(self.cbz_path.is_file());

        let extract_dir = get_extraction_root_dir(&self.cbz_path);

        let file = File::open(&self.cbz_path).unwrap();
        let reader = BufReader::new(file);
        let mut archive = ZipArchive::new(reader).unwrap();

        debug!("extracting {:?} to {:?}", self.cbz_path, extract_dir);
        fs::create_dir_all(extract_dir.clone()).unwrap();
        archive.extract(extract_dir.clone()).unwrap();
    }

    fn compress_cbz(&mut self) {
        trace!("called compress_cbz() with {:?}", self.cbz_path);

        let dir = self.cbz_path.parent().unwrap();
        let name = self.cbz_path.file_stem().unwrap();
        let zip_path = match self.target_format {
            ImageFormat::Avif => dir.join(format!("{}.avif.cbz", name.to_str().unwrap())),
            ImageFormat::Jxl => dir.join(format!("{}.jxl.cbz", name.to_str().unwrap())),
            ImageFormat::Jpeg => todo!(),
            ImageFormat::Png => todo!(),
        };
        debug!("create cbz at {:?}", zip_path);
        let file = File::create(zip_path).unwrap();

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        let extract_dir = get_conversion_root_dir(&self.cbz_path);
        let mut buffer = Vec::new();
        for entry in WalkDir::new(&extract_dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let entry = entry.path();
            debug!("add to archive: {:?}", entry);
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

    fn run(mut self) -> Result<(), ConversionError> {
        debug!("start conversion for {:?}", self.cbz_path);

        assert!(!self.job_queue.is_empty());
        self.extract_cbz();

        // these signals will be catched from here on out until the end of this function
        let mut signals = match Signals::new(&[SIGINT, SIGCHLD]) {
            Ok(signals) => signals,
            Err(_) => {
                return Err(ConversionError::Unspecific(
                    "could not listen to signals".to_string(),
                ))
            }
        };

        // start out as many jobs as allowed
        trace!("start initial jobs");
        while self.jobs_in_process.len() < self.workers {
            let mut job = match self.job_queue.pop_front() {
                Some(job) => job,
                None => break,
            };

            let status = job.proceed()?;
            match status {
                JobStatus::Converting => {
                    self.jobs_in_process.push(job);
                }
                JobStatus::Done => (),
                JobStatus::Init => unreachable!(),
            }
        }

        // add new jobs as other jobs complete
        trace!("start new jobs as old ones complete");
        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => {
                        debug!("got signal SIGINT");
                        return Err(ConversionError::Interrupt);
                    }
                    SIGCHLD => {
                        debug!("got signal SIGCHLD");
                        self.proceed_jobs()?;
                        if !self.job_queue.is_empty() {
                            self.start_next_jobs()?;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        self.compress_cbz();
        Ok(())
    }

    fn proceed_jobs(&mut self) -> Result<(), ConversionError> {
        trace!("proceed all ready jobs");
        for job in self.jobs_in_process.iter_mut() {
            trace!("job in process: {job:?}");
            if job.can_proceed()? {
                match job.proceed()? {
                    JobStatus::Done => (),
                    JobStatus::Init | JobStatus::Converting => unreachable!(),
                }
            }
        }
        Ok(())
    }

    fn start_next_jobs(&mut self) -> Result<(), ConversionError> {
        trace!("start new jobs");
        for job in self.jobs_in_process.iter_mut() {
            trace!("job in process: {job:?}");
            if let JobStatus::Done = job.status {
                let mut new_job = self.job_queue.pop_front().unwrap();
                trace!("replace job {job:?} for {new_job:?}");
                std::mem::swap(job, &mut new_job);
                job.proceed()?;
            }
        }
        Ok(())
    }

    fn jobs_pending(&self) -> bool {
        let all_done = self
            .jobs_in_process
            .iter()
            .map(|job| job.status)
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
        f.debug_struct("ConversionJob")
            .field("status", &self.status)
            .field("from", &self.current)
            .field("to", &self.target)
            .field("image_path", &self.image_path.to_string_lossy())
            .finish()
    }
}

impl Drop for ConversionJob {
    fn drop(&mut self) {
        trace!("drop {self:?}");
        let mut child = match self.child.take() {
            Some(child) => child,
            None => return,
        };

        // ignore errors
        let _ = child.kill();
        let _ = child.wait(); // is this necessary?
    }
}

impl Drop for WorkUnit {
    fn drop(&mut self) {
        debug!("cleanup for {:?}", self.cbz_path);
        let extract_dir = get_conversion_root_dir(&self.cbz_path);
        if extract_dir.exists() {
            fs::remove_dir_all(extract_dir.clone()).unwrap();
        }
    }
}

fn images_in_archive(cbz_path: &PathBuf) -> Result<Vec<(PathBuf, ImageFormat)>, ConversionError> {
    trace!("called cbz_contains_convertable_images()");

    let file = File::open(cbz_path).unwrap();
    let reader = BufReader::new(file);

    let archive = match ZipArchive::new(reader) {
        Ok(archive) => archive,
        Err(_) => return Err(ConversionError::NotAnArchive(cbz_path.clone())),
    };
    let mut result = vec![];
    for file_inside in archive.file_names() {
        let file_inside = PathBuf::from(file_inside);
        trace!("found file {:?}", file_inside);
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

fn get_extraction_root_dir(cbz_path: &PathBuf) -> PathBuf {
    let file = File::open(&cbz_path).unwrap();
    let reader = BufReader::new(file);
    let archive = ZipArchive::new(reader).unwrap();

    let archive_name = cbz_path.file_stem().unwrap();
    let archive_root_dirs = archive
        .file_names()
        .into_iter()
        .filter(|s| s.ends_with("/"))
        .filter(|s| s.find("/").unwrap() == s.len() - 1)
        .collect::<Vec<_>>();

    let has_root_within = archive_root_dirs.len() == 1;
    let extract_dir = if has_root_within {
        trace!("extract directly");
        let parent_dir = cbz_path.parent().unwrap().to_path_buf();
        assert_eq!(
            parent_dir.join(archive_name),
            get_conversion_root_dir(&cbz_path)
        );
        parent_dir
    } else {
        trace!("extract into new root directory");
        get_conversion_root_dir(&cbz_path)
    };
    extract_dir
}

fn get_conversion_root_dir(cbz_path: &PathBuf) -> PathBuf {
    let dir = cbz_path.parent().unwrap();
    let name = cbz_path.file_stem().unwrap();
    let root_dir = dir.join(name);
    root_dir
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

fn convert_single_cbz(
    cbz_file: &PathBuf,
    format: ImageFormat,
    workers: usize,
) -> Result<(), ConversionError> {
    trace!("called convert_single_cbz() with {:?}", cbz_file);
    if already_converted(&cbz_file, format) {
        return Err(ConversionError::AlreadyDone(cbz_file.to_path_buf()));
    }

    let work_unit = WorkUnit::new(cbz_file.clone(), format, workers)?;
    work_unit.run()
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
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_secs()
        .parse_env("RUST_LOG")
        .init();

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
                info!("Converting {:?}", cbz_file);
                if let Err(e) = convert_single_cbz(&cbz_file, format, workers) {
                    warn!("{e}");
                } else {
                    info!("Done");
                }
            }
        }
    } else {
        if let Err(e) = convert_single_cbz(&path, format, workers) {
            match e {
                ConversionError::NothingToDo(_) => info!("Nothing to do for {path:?}"),
                ConversionError::NotAnArchive(_) => info!("This is not a Zip archive"),
                _ => error!("{e}"),
            }
        }
    }
    Ok(())
}
