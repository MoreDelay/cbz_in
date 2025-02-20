mod spawn;

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{exit, Child, Command, Stdio};
use std::thread;

use anyhow::Result;
use clap::Parser;
use log::{debug, error, info, trace};
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use walkdir::WalkDir;
use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

#[derive(Error, Debug)]
enum ConversionError {
    #[error("not an archive '{0}'")]
    NotAnArchive(PathBuf),
    #[error("nothing to do for '{0}'")]
    NothingToDo(PathBuf),
    #[error("conversion not supported from {0:?} to {1:?}")]
    NotSupported(ImageFormat, ImageFormat),
    #[error("conversion already done for '{0}'")]
    AlreadyDone(PathBuf),
    #[error("got interrupted")]
    Interrupt,
    #[error("Error during extraction: {0}")]
    ExtractionError(String),
    #[error("child process finished abnormally for '{0}'")]
    AbnormalExit(PathBuf),
    #[error("could not start process with the program '{0}'")]
    SpawnFailure(String),
    #[error("unspecific error '{0}'")]
    Unspecific(String),
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

impl std::fmt::Display for ImageFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Jpeg => write!(f, "jpeg"),
            Png => write!(f, "png"),
            Avif => write!(f, "avif"),
            Jxl => write!(f, "jxl"),
            Webp => write!(f, "webp"),
        }
    }
}

#[derive(Default, Clone, Copy, Debug, PartialEq)]
enum JobStatus {
    Init,
    Decoding,
    Encoding,
    #[default]
    Done,
}

struct ConversionJob {
    status: JobStatus,
    image_path: PathBuf,
    current: ImageFormat,
    intermediate: Option<ImageFormat>,
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
        match (from, to) {
            (a, b) if a == b => return Err(NotSupported(from, to)),
            (_, Jpeg | Png | Avif | Jxl | Webp) => (),
        }

        Ok(ConversionJob {
            status: JobStatus::Init,
            image_path,
            current: from,
            intermediate: None,
            target: to,
            child: None,
        })
    }

    fn on_init(&mut self) -> Result<JobStatus, ConversionError> {
        let next_status = match (self.current, self.target) {
            (Jpeg, to @ Png) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::convert_jpeg_to_png(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Png, to @ Jpeg) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::convert_png_to_jpeg(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Jpeg | Png, to @ Avif) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::encode_avif(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Jpeg | Png, to @ Jxl) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::encode_jxl(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Jpeg | Png, to @ Webp) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::encode_webp(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Avif, to @ Jpeg) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::decode_avif_to_jpeg(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Avif, to @ Png) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::decode_avif_to_png(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Jxl, to @ Jpeg) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::decode_jxl_to_jpeg(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Jxl, to @ Png) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::decode_jxl_to_png(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Webp, to @ Png) => {
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::decode_webp(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (Avif, Jxl | Webp) => {
                self.intermediate = Some(Png);
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(Png.to_string());
                let child = spawn::decode_avif_to_png(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Decoding
            }
            (Jxl, Avif | Webp) => {
                let input_path = &self.image_path;
                let child = if jxl_is_compressed_jpeg(&self.image_path)? {
                    self.intermediate = Some(Jpeg);
                    let output_path = self.image_path.with_extension(Jpeg.to_string());
                    spawn::decode_jxl_to_jpeg(input_path, &output_path)?
                } else {
                    self.intermediate = Some(Png);
                    let output_path = self.image_path.with_extension(Png.to_string());
                    spawn::decode_jxl_to_png(input_path, &output_path)?
                };
                self.child = Some(child);
                JobStatus::Decoding
            }
            (Webp, Jpeg | Avif | Jxl) => {
                self.intermediate = Some(Png);
                let input_path = &self.image_path;
                let output_path = self.image_path.with_extension(Png.to_string());
                let child = spawn::decode_webp(input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Decoding
            }
            (Jpeg, Jpeg) => JobStatus::Done,
            (Png, Png) => JobStatus::Done,
            (Avif, Avif) => JobStatus::Done,
            (Jxl, Jxl) => JobStatus::Done,
            (Webp, Webp) => JobStatus::Done,
        };
        self.status = next_status;
        Ok(next_status)
    }

    fn on_decoding(&mut self) -> Result<JobStatus, ConversionError> {
        let child: &mut Child = match &mut self.child {
            Some(child) => child,
            None => unreachable!(),
        };
        match child.wait() {
            Ok(status) if !status.success() => {
                let output = extract_console_output(child);
                debug!("error on process:\n{output}");
                return Err(AbnormalExit(self.image_path.clone()));
            }
            Ok(_) => {
                let output = extract_console_output(child);
                trace!("process output:\n{output}");
            }
            Err(_) => return Err(Unspecific("error during wait".to_string())),
        }

        if fs::remove_file(&self.image_path).is_err() {
            return Err(Unspecific(format!(
                "intermediate step: Could not delete '{:?}'",
                self.image_path
            )));
        }

        let next_status = match (self.intermediate.unwrap(), self.target) {
            (from @ (Jpeg | Png), to @ Avif) => {
                let input_path = self.image_path.with_extension(from.to_string());
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::encode_avif(&input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (from @ (Jpeg | Png), to @ Jxl) => {
                let input_path = self.image_path.with_extension(from.to_string());
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::encode_jxl(&input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (from @ Png, to @ Jpeg) => {
                let input_path = self.image_path.with_extension(from.to_string());
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::convert_png_to_jpeg(&input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (from @ Png, to @ Webp) => {
                let input_path = self.image_path.with_extension(from.to_string());
                let output_path = self.image_path.with_extension(to.to_string());
                let child = spawn::encode_webp(&input_path, &output_path)?;
                self.child = Some(child);
                JobStatus::Encoding
            }
            (_, Jpeg | Png | Avif | Jxl | Webp) => unreachable!(),
        };
        self.status = next_status;
        Ok(next_status)
    }

    // wait on child process and delete original image file
    fn on_encoding(&mut self) -> Result<JobStatus, ConversionError> {
        let child: &mut Child = match &mut self.child {
            Some(child) => child,
            None => unreachable!(),
        };
        match child.wait() {
            Ok(status) if !status.success() => {
                let output = extract_console_output(child);
                debug!("error on process:\n{output}");
                return Err(AbnormalExit(self.image_path.clone()));
            }
            Ok(_) => {
                let output = extract_console_output(child);
                trace!("process output:\n{output}");
            }
            Err(_) => return Err(Unspecific("error during wait".to_string())),
        }
        let delete_path = match self.intermediate {
            Some(intermediate) => self.image_path.with_extension(intermediate.to_string()),
            None => self.image_path.clone(),
        };

        self.status = JobStatus::Done;
        match fs::remove_file(&delete_path) {
            Ok(_) => Ok(self.status),
            Err(_) => Err(Unspecific(format!(
                "converting step: Could not delete '{:?}'",
                delete_path
            ))),
        }
    }

    fn proceed(&mut self) -> Result<JobStatus, ConversionError> {
        debug!("proceed with {self:?}");
        let result = match self.status {
            JobStatus::Init => self.on_init(),
            JobStatus::Decoding => self.on_decoding(),
            JobStatus::Encoding => self.on_encoding(),
            JobStatus::Done => Ok(JobStatus::Done),
        };
        debug!("after proceed {self:?}");
        result
    }

    fn can_proceed(&mut self) -> Result<bool, ConversionError> {
        match self.status {
            JobStatus::Init => unreachable!(),
            JobStatus::Decoding => (),
            JobStatus::Encoding => (),
            JobStatus::Done => return Ok(false),
        }
        let child: &mut Child = match &mut self.child {
            Some(child) => child,
            None => unreachable!(),
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
            Err(_) => {
                trace!("error");
                Err(Unspecific(self.image_path.to_string_lossy().to_string()))
            }
        }
    }
}

impl WorkUnit {
    fn new(
        cbz_path: &Path,
        target_format: ImageFormat,
        workers: usize,
        force: bool,
    ) -> Result<WorkUnit, ConversionError> {
        let cbz_path = cbz_path.to_path_buf();
        trace!("called WorkUnit::new()");
        let not_correct_extention = cbz_path
            .extension()
            .is_none_or(|e| e != "cbz" && e != "zip");
        if not_correct_extention {
            return Err(NotAnArchive(cbz_path.to_path_buf()));
        }

        let root_dir = get_extraction_root_dir(&cbz_path);
        let job_queue = images_in_archive(&cbz_path)?
            .iter()
            .filter_map(|(image_path, format)| {
                ConversionJob::new(root_dir.join(image_path), *format, target_format).ok()
            })
            .filter(|job| force || !convert_only_when_forced(job.current, job.target))
            .collect::<VecDeque<_>>();
        if job_queue.is_empty() {
            return Err(NothingToDo(cbz_path));
        }

        Ok(WorkUnit {
            cbz_path,
            job_queue,
            jobs_in_process: vec![],
            target_format,
            workers,
        })
    }

    fn extract_cbz(&mut self) -> Result<(), ConversionError> {
        trace!("called extract_cbz() with {:?}", self.cbz_path);
        assert!(self.cbz_path.is_file());

        let extract_dir = get_conversion_root_dir(&self.cbz_path);

        debug!("extracting {:?} to {:?}", self.cbz_path, extract_dir);
        if extract_dir.exists() {
            return Err(ConversionError::ExtractionError(
                "Extract directory already exists, delete it and try again".to_string(),
            ));
        }
        fs::create_dir_all(&extract_dir).unwrap();

        let mut command = Command::new("7z");
        command.args([
            "x",
            "-tzip", // undocumented switch to remove header lines
            self.cbz_path.to_str().unwrap(),
            "-spe",
            format!("-o{}", extract_dir.to_str().unwrap()).as_str(),
        ]);
        let child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| SpawnFailure("7z".to_string()))
            .unwrap();

        match child.wait_with_output() {
            Ok(output) if output.status.code().is_some_and(|code| code == 0) => Ok(()),
            Ok(_) => Err(ConversionError::ExtractionError(
                "Extraction with 7z unsuccessful".to_string(),
            )),
            Err(e) => Err(ConversionError::ExtractionError(e.to_string())),
        }
    }

    fn compress_cbz(&mut self) {
        trace!("called compress_cbz() with {:?}", self.cbz_path);

        let dir = self.cbz_path.parent().unwrap();
        let name = self.cbz_path.file_stem().unwrap();
        let zip_path = dir.join(format!(
            "{}.{}.cbz",
            name.to_str().unwrap(),
            self.target_format
        ));
        debug!("create cbz at {:?}", zip_path);
        let file = File::create(zip_path).unwrap();

        let mut zipper = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o755);

        let extract_dir = get_conversion_root_dir(&self.cbz_path);
        trace!("compress directory {extract_dir:?}");
        let mut buffer = Vec::new();
        for entry in WalkDir::new(&extract_dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let entry = entry.path();
            debug!("add to archive: {:?}", entry);
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
    }

    fn run(mut self) -> Result<(), ConversionError> {
        debug!("start conversion for {:?}", self.cbz_path);

        assert!(!self.job_queue.is_empty());
        self.extract_cbz()?;

        // these signals will be catched from here on out until the end of this function
        let mut signals = match Signals::new([SIGINT, SIGCHLD]) {
            Ok(signals) => signals,
            Err(_) => return Err(Unspecific("could not listen to signals".to_string())),
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
                JobStatus::Init => unreachable!(),
                JobStatus::Decoding => self.jobs_in_process.push(job),
                JobStatus::Encoding => self.jobs_in_process.push(job),
                JobStatus::Done => (),
            }
        }

        trace!("start new jobs as old ones complete");
        while self.jobs_pending() {
            for signal in signals.wait() {
                match signal {
                    SIGINT => {
                        debug!("got signal SIGINT");
                        return Err(Interrupt);
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
                    JobStatus::Init => unreachable!(),
                    JobStatus::Decoding => unreachable!(),
                    JobStatus::Encoding => (),
                    JobStatus::Done => (),
                }
            }
        }
        Ok(())
    }

    fn start_next_jobs(&mut self) -> Result<(), ConversionError> {
        trace!("start new jobs");
        'replace: for job in self.jobs_in_process.iter_mut() {
            trace!("job in process: {job:?}");
            if let JobStatus::Done = job.status {
                let mut new_job = 'search: loop {
                    let mut new_job = match self.job_queue.pop_front() {
                        Some(new_job) => new_job,
                        None => break 'replace,
                    };
                    match new_job.proceed()? {
                        JobStatus::Done => continue,
                        _ => break 'search new_job,
                    }
                };
                trace!("replace job {job:?} for {new_job:?}");
                std::mem::swap(job, &mut new_job);
            }
        }
        Ok(())
    }

    fn jobs_pending(&self) -> bool {
        let all_done = self
            .jobs_in_process
            .iter()
            .map(|job| job.status)
            .all(|status| JobStatus::Done == status);
        !all_done
    }
}

impl std::fmt::Debug for ConversionJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut writer = f.debug_struct("ConversionJob");
        writer
            .field("status", &self.status)
            .field("from", &self.current)
            .field("to", &self.target);
        if let Some(inter) = self.intermediate {
            writer.field("over", &inter);
        }
        writer
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
    let mut command = Command::new("jxlinfo");
    command.args(["-v", image_path.to_str().unwrap()]);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("jxlinfo".to_string()))?;

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
        Err(_) => Err(Unspecific("error during wait".to_string())),
    }
}

fn images_in_archive(cbz_path: &Path) -> Result<Vec<(PathBuf, ImageFormat)>, ConversionError> {
    trace!("called images_in_archive()");

    let mut command = Command::new("7z");
    command.args([
        "l",
        "-ba",  // undocumented switch to remove header lines
        "-slt", // use format that is easier to parse
        cbz_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("7z".to_string()))?;
    match child.wait_with_output() {
        Ok(output) => {
            let files = output
                .stdout
                .lines()
                .filter(|v| v.as_ref().is_ok_and(|line| line.starts_with("Path = ")))
                .map(|v| v.unwrap().strip_prefix("Path = ").unwrap().to_string())
                .map(PathBuf::from)
                .filter_map(|file| {
                    trace!("found file {file:?}");
                    match file.extension()?.to_str().unwrap() {
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
        Err(e) => Err(ConversionError::Unspecific(e.to_string())),
    }
}

fn get_extraction_root_dir(cbz_path: &Path) -> PathBuf {
    let mut command = Command::new("7z");
    command.args([
        "l",
        "-ba",  // undocumented switch to remove header lines
        "-slt", // use format that is easier to parse
        cbz_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("7z".to_string()))
        .unwrap();

    let archive_name = cbz_path.file_stem().unwrap();
    let archive_root_dirs = match child.wait_with_output() {
        Ok(output) => output
            .stdout
            .lines()
            .filter(|v| v.as_ref().is_ok_and(|line| line.starts_with("Path = ")))
            .map(|v| v.unwrap().strip_prefix("Path = ").unwrap().to_string())
            .filter(|file| !file.contains("/"))
            .collect::<Vec<_>>(),
        Err(e) => panic!("{:?}", ConversionError::Unspecific(e.to_string())),
    };

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
    extract_dir
}

fn get_conversion_root_dir(cbz_path: &Path) -> PathBuf {
    let dir = cbz_path.parent().unwrap();
    let name = cbz_path.file_stem().unwrap();
    dir.join(name)
}

fn already_converted(path: &Path, format: ImageFormat) -> bool {
    let conversion_ending = format!(".{}.cbz", format);

    let dir = path.parent().unwrap();
    let name = path.file_stem().unwrap();
    let zip_path = dir.join(format!("{}{}", name.to_str().unwrap(), conversion_ending));

    let is_converted_archive = path.to_str().unwrap().ends_with(&conversion_ending);
    let has_converted_archive = zip_path.exists();

    trace!(" is converted archive? {is_converted_archive}");
    trace!("has converted archive? {has_converted_archive}");
    is_converted_archive || has_converted_archive
}

fn convert_single_cbz(
    cbz_file: &Path,
    format: ImageFormat,
    workers: usize,
    force: bool,
) -> Result<(), ConversionError> {
    trace!("called convert_single_cbz() with {:?}", cbz_file);
    if already_converted(cbz_file, format) {
        return Err(AlreadyDone(cbz_file.to_path_buf()));
    }

    let work_unit = WorkUnit::new(cbz_file, format, workers, force)?;
    work_unit.run()
}

fn convert_only_when_forced(from: ImageFormat, to: ImageFormat) -> bool {
    match (from, to) {
        (Jpeg | Png, _) => false,
        (_, Jpeg | Png) => false,
        (_, _) => true,
    }
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
        error!("does not exists: {:?}", path);
        exit(1);
    }

    let workers = match matches.workers {
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
            info!("Converting {:?}", cbz_file);
            match convert_single_cbz(&cbz_file, format, workers, force) {
                Ok(()) => info!("Done"),
                Err(NothingToDo(path)) => info!("Nothing to do for {path:?}"),
                Err(AlreadyDone(path)) => info!("Already converted {path:?}"),
                Err(NotAnArchive(_)) => info!("This is not a Zip archive"),
                Err(e) => {
                    error!("{e}");
                    break;
                }
            }
        }
    } else if let Err(e) = convert_single_cbz(&path, format, workers, force) {
        match e {
            NothingToDo(_) => info!("Nothing to do for {path:?}"),
            NotAnArchive(_) => info!("This is not a Zip archive"),
            _ => error!("{e}"),
        }
    }
    Ok(())
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
