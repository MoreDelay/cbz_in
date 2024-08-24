use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{exit, Child, Command, ExitStatus, Stdio};
use std::thread;

use anyhow::{anyhow, Result};
use clap::Parser;
use log::{debug, error, info, trace};
use signal_hook::{
    consts::{SIGCHLD, SIGINT},
    iterator::Signals,
};
use thiserror::Error;
use walkdir::WalkDir;
use zip::{write::SimpleFileOptions, CompressionMethod, ZipArchive, ZipWriter};

struct WorkUnit {
    cbz_path: PathBuf,
    format: ImageFormats,
}

impl WorkUnit {
    fn new(path: &Path, format: ImageFormats) -> WorkUnit {
        WorkUnit {
            cbz_path: path.to_path_buf(),
            format,
        }
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

fn extract_dir_from_cbz_path(path: &Path) -> PathBuf {
    let dir = path.parent().unwrap();
    let name = path.file_stem().unwrap();
    let extract_dir = dir.join(name);
    extract_dir
}

fn cbz_contains_convertable_images(path: &Path) -> bool {
    trace!("Called cbz_contains_convertable_images()");

    if let None = path.extension() {
        debug!("No extension");
        return false;
    }
    if path.extension().map_or(false, |e| e != "cbz") {
        debug!("Wrong extension");
        return false;
    }

    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    let archive = ZipArchive::new(reader).unwrap();
    for file_inside in archive.file_names() {
        let file_inside = Path::new(file_inside);
        trace!("Looking at file: {:?}", file_inside);
        if let Some(ext) = file_inside.extension() {
            match ext.to_str().unwrap() {
                "jpg" => return true,
                "jpeg" => return true,
                "png" => return true,
                _ => (),
            }
        }
    }

    false
}

fn already_converted(path: &PathBuf) -> bool {
    let dir = path.parent().unwrap();
    let name = path.file_stem().unwrap();
    let zip_path = dir.join(format!("{}.avif.cbz", name.to_str().unwrap()));
    zip_path.exists()
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

fn extract_cbz(work_unit: &WorkUnit) {
    let cbz_path = &work_unit.cbz_path;
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
}

fn start_conversion_process(image_path: &Path, format: ImageFormats) -> Result<Child> {
    debug!("New process working on {:?}", image_path);

    match format {
        ImageFormats::Avif => {
            let avif_path = image_path.with_extension("avif");
            let mut command = Command::new("cavif");
            command.args([
                "--speed=3",
                "--threads=1",
                "--quality=88",
                image_path.to_str().unwrap(),
                "-o",
                avif_path.to_str().unwrap(),
            ]);
            let child = command
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|_| CbzError::ProcessSpawnError("cavif".to_string()))?;
            Ok(child)
        }
        ImageFormats::Jxl => {
            let jxl_path = image_path.with_extension("jxl");
            let mut command = Command::new("cjxl");
            command.args([
                "--effort=9",
                "--num_threads=1",
                "--distance=0",
                image_path.to_str().unwrap(),
                jxl_path.to_str().unwrap(),
            ]);
            let child = command
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|_| CbzError::ProcessSpawnError("cjxl".to_string()))?;
            Ok(child)
        }
    }
}

struct ConversionTask(PathBuf, Child);

#[derive(Error, Debug)]
enum CbzError {
    #[error("No task completed")]
    NoTaskCompleted,
    #[error("Conversion failed for {0} with status {1:?}")]
    ConversionFailed(String, ExitStatus),
    #[error("IO Error")]
    IOError,
    #[error("Could not spawn process for conversion program: {0}")]
    ProcessSpawnError(String),
}

fn find_completed_process(
    running_tasks: &mut Vec<ConversionTask>,
) -> Result<&mut ConversionTask, CbzError> {
    for task in running_tasks.iter_mut() {
        let ConversionTask(task_image_path, child) = task;
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(task);
                } else {
                    return Err(CbzError::ConversionFailed(
                        task_image_path.to_str().unwrap().to_string(),
                        status,
                    ));
                }
            }
            Ok(None) => continue,
            Err(_) => return Err(CbzError::IOError),
        }
    }
    Err(CbzError::NoTaskCompleted)
}

fn start_next_conversion_after_another_completes(
    running_tasks: &mut Vec<ConversionTask>,
    signals: &mut Signals,
    image_path: &Path,
    format: ImageFormats,
) -> Result<()> {
    loop {
        for signal in signals.wait() {
            match signal {
                SIGINT => {
                    debug!("Got signal SIGINT");
                    return Err(anyhow!("Interrupted"));
                }
                SIGCHLD => {
                    debug!("Got signal SIGCHLD");
                    // search for all completed processes and start new ones in their place
                    loop {
                        match find_completed_process(running_tasks) {
                            Ok(task) => {
                                let ConversionTask(task_image_path, ref mut child) = task;

                                child.wait().unwrap();
                                fs::remove_file(task_image_path)?;
                                let child = start_conversion_process(image_path, format)?;
                                *task = ConversionTask(image_path.to_path_buf(), child);
                                return Ok(());
                            }
                            Err(CbzError::NoTaskCompleted) => break,
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

fn wait_for_children(running_tasks: &mut Vec<ConversionTask>) -> Result<()> {
    let mut result = Ok(());

    for task in running_tasks.iter_mut() {
        let ConversionTask(task_image_path, child) = task;
        match child.wait() {
            Ok(status) => {
                if status.success() {
                    fs::remove_file(task_image_path)?;
                } else {
                    result = Err(anyhow!(
                        "Conversion failed for {} because: {}",
                        task_image_path.to_str().unwrap(),
                        status
                    ));
                    break;
                }
            }
            Err(error) => {
                result = Err(anyhow!("Some error occured: {}", error));
                break;
            }
        }
    }
    kill_all_children(running_tasks);
    result
}

fn kill_all_children(running_tasks: &mut Vec<ConversionTask>) {
    for ConversionTask(_, ref mut child) in running_tasks {
        let _ = child.kill();
        child.wait().unwrap();
    }
}

fn convert_images(work_unit: &WorkUnit, process_count: usize) -> Result<()> {
    let cbz_path = &work_unit.cbz_path;
    let format = work_unit.format;
    trace!("Called convert_images with {:?}", cbz_path);
    debug!("Start converting images for {:?}", cbz_path);
    let extract_dir = extract_dir_from_cbz_path(cbz_path);
    let mut running_tasks = Vec::new();

    // from here on out we catch these signals until this function leaves
    let mut signals = Signals::new(&[SIGINT, SIGCHLD])?;

    let mut result = Ok(());
    for entry in WalkDir::new(&extract_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let entry = entry.path();
        if entry.is_file()
            && (entry.extension() == Some(OsStr::new("jpg"))
                || entry.extension() == Some(OsStr::new("jpeg"))
                || entry.extension() == Some(OsStr::new("png")))
        {
            let image_path = entry;

            if running_tasks.len() < process_count {
                let child = start_conversion_process(image_path, format)?;
                running_tasks.push(ConversionTask(image_path.to_path_buf(), child));
                continue;
            }

            result = start_next_conversion_after_another_completes(
                &mut running_tasks,
                &mut signals,
                image_path,
                format,
            );
            if result.is_err() {
                break;
            }
        }
    }
    if result.is_ok() {
        result = wait_for_children(&mut running_tasks);
    } else {
        kill_all_children(&mut running_tasks);
    }
    result
}

fn compress_cbz(work_unit: &WorkUnit) {
    let cbz_path = &work_unit.cbz_path;
    trace!("Called compress_cbz() with {:?}", cbz_path);

    let dir = cbz_path.parent().unwrap();
    let name = cbz_path.file_stem().unwrap();
    let zip_path = match work_unit.format {
        ImageFormats::Avif => dir.join(format!("{}.avif.cbz", name.to_str().unwrap())),
        ImageFormats::Jxl => dir.join(format!("{}.jxl.cbz", name.to_str().unwrap())),
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
    cbz_file: PathBuf,
    format: ImageFormats,
    use_processors: usize,
) -> Result<()> {
    trace!("Called convert_single_cbz() with {:?}", cbz_file);
    if !cbz_contains_convertable_images(&cbz_file) {
        info!("Nothing to do for {:?}", cbz_file);
        return Ok(());
    }
    if already_converted(&cbz_file) {
        info!("Conversion already exists");
        return Ok(());
    }

    info!("Converting {:?}", cbz_file);

    // Using work unit struct to do cleanup on drop
    let work_unit = WorkUnit::new(&cbz_file, format);
    extract_cbz(&work_unit);
    //if there was any error, we interrupt the whole process without saving
    convert_images(&work_unit, use_processors)?;
    compress_cbz(&work_unit);
    Ok(())
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum ImageFormats {
    Avif,
    Jxl,
}

#[derive(Parser)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(
        required = true,
        help = "All images within the archive(s) are converted to this format"
    )]
    format: ImageFormats,

    #[arg(
        required = true,
        default_value = ".",
        help = "Path to a cbz file or a directory containing cbz files"
    )]
    path: PathBuf,
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

    let use_processors = match thread::available_parallelism() {
        Ok(value) => value.get(),
        Err(_) => 1,
    };

    if path.is_dir() {
        for cbz_file in path.read_dir().expect("read dir call failed!") {
            if let Ok(cbz_file) = cbz_file {
                let cbz_file = cbz_file.path();
                debug!("Next path: {:?}", cbz_file);
                if let Err(e) = convert_single_cbz(cbz_file, format, use_processors) {
                    error!("{e}");
                    break;
                }
            }
        }
    } else {
        if let Err(e) = convert_single_cbz(path, format, use_processors) {
            error!("{e}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extraction_different_name() {
        let cbz_path = Path::new("data/Test1.cbz");
        let unit = WorkUnit::new(cbz_path, ImageFormats::Avif);

        extract_cbz(&unit);

        let extract_root = Path::new("data/Test1");
        assert!(extract_root.is_dir());
        let extract_inner = Path::new("data/Test1/Test");
        assert!(extract_inner.is_dir());
        let not_inner = Path::new("data/Test1/Test1");
        assert!(!not_inner.exists());
    }

    #[test]
    fn test_extraction_same_name() {
        let cbz_path = Path::new("data/Test.cbz");
        let unit = WorkUnit::new(cbz_path, ImageFormats::Avif);

        extract_cbz(&unit);

        let extract_root = Path::new("data/Test");
        assert!(extract_root.is_dir());
        let not_inner = Path::new("data/Test/Test");
        assert!(!not_inner.exists());
    }
}
