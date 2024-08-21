use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::exit;

use anyhow::Result;
use load_image::ImageData::{GRAY16, GRAY8, GRAYA16, GRAYA8, RGB16, RGB8, RGBA16, RGBA8};
use log::{debug, error, info, trace};
use rayon::prelude::*;
use rgb::ComponentMap;
use walkdir::WalkDir;
use zip;
use zip::write::SimpleFileOptions;

struct WorkUnit {
    cbz_path: PathBuf,
}

impl WorkUnit {
    fn new(path: &Path) -> WorkUnit {
        WorkUnit {
            cbz_path: path.to_path_buf(),
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
    debug!("Called cbz_contains_convertable_images()");
    debug!("path = {:?}", path);

    if let None = path.extension() {
        debug!("No extension");
        return false;
    }
    if let Some(ext) = path.extension() {
        if ext != "cbz" {
            debug!("Wrong extension");
            return false;
        }
    }

    let file = fs::File::open(path).unwrap();
    let reader = BufReader::new(file);

    let archive = zip::ZipArchive::new(reader).unwrap();
    for file_inside in archive.file_names() {
        let file_inside = Path::new(file_inside);
        debug!("Inside: {:?}", file_inside);
        if let Some(ext) = file_inside.extension() {
            match ext.to_str().unwrap() {
                "jpg" => return true,
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

fn extract_cbz(work_unit: &WorkUnit) {
    let cbz_path = &work_unit.cbz_path;
    assert!(cbz_path.is_file());

    let file = fs::File::open(cbz_path).unwrap();
    let reader = BufReader::new(file);
    let mut archive = zip::ZipArchive::new(reader).unwrap();
    let extract_dir = extract_dir_from_cbz_path(cbz_path);

    debug!("Extracting {:?} to {:?}", cbz_path, extract_dir);
    fs::create_dir_all(extract_dir.clone()).unwrap();
    archive.extract(extract_dir.clone()).unwrap();
}

fn convert_image(image_path: &Path) -> Result<()> {
    let image = load_image::load_path(image_path)?;

    let image_data = match image.bitmap {
        RGB8(data) => {
            trace!("RGB8");
            data.into_iter().map(|p| p.with_alpha(255)).collect()
        }
        RGBA8(data) => {
            trace!("RGBA8");
            data
        }
        RGB16(data) => {
            trace!("RGB16");
            data.into_iter()
                .map(|p| p.map(|v| (v >> 8) as u8).with_alpha(255))
                .collect()
        }
        RGBA16(data) => {
            trace!("RGBA16");
            data.into_iter()
                .map(|p| p.map(|v| (v >> 8) as u8))
                .collect()
        }
        GRAY8(data) => {
            trace!("GRAY8");
            data.into_iter()
                .map(|p| rgb::RGBA::new(p.0, p.0, p.0, 255))
                .collect()
        }
        GRAYA8(data) => {
            trace!("GRAYA8");
            data.into_iter()
                .map(|p| rgb::RGBA::new(p.v, p.v, p.v, p.a))
                .collect()
        }
        GRAY16(data) => {
            trace!("GRAY16");
            data.into_iter()
                .map(|p| {
                    let v = (p.0 >> 8) as u8;
                    rgb::RGBA::new(v, v, v, 255)
                })
                .collect()
        }
        GRAYA16(data) => {
            trace!("GRAYA16");
            data.into_iter()
                .map(|p| {
                    let v = (p.v >> 8) as u8;
                    let a = (p.a >> 8) as u8;
                    rgb::RGBA::new(v, v, v, a)
                })
                .collect()
        }
    };

    let avif_path = image_path.with_extension("avif");
    let encoder = ravif::Encoder::new()
        .with_speed(3)
        .with_num_threads(Some(1))
        .with_quality(88.);

    let result = encoder.encode_rgba(ravif::Img::new(&image_data, image.width, image.height))?;
    fs::write(avif_path, result.avif_file)?;
    fs::remove_file(image_path)?;
    Ok(())
}

fn convert_images(work_unit: &WorkUnit) {
    let cbz_path = &work_unit.cbz_path;
    debug!("Convert images for {:?}", cbz_path);
    let extract_dir = extract_dir_from_cbz_path(cbz_path);
    let recursive_root = extract_dir.join(cbz_path.file_stem().unwrap());
    WalkDir::new(recursive_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .collect::<Vec<_>>()
        .par_iter()
        .for_each(|entry| {
            let entry = entry.path();
            debug!("Found file: {:?}", entry);
            if entry.is_file() && entry.extension() == Some(OsStr::new("jpg")) {
                convert_image(entry).unwrap();
            }
        })
}

fn compress_cbz(work_unit: &WorkUnit) {
    let cbz_path = &work_unit.cbz_path;
    debug!("Compress cbz for {:?}", cbz_path);

    let dir = cbz_path.parent().unwrap();
    let name = cbz_path.file_stem().unwrap();
    let zip_path = dir.join(format!("{}.avif.cbz", name.to_str().unwrap()));
    debug!("Create cbz at {:?}", zip_path);
    let file = fs::File::create(zip_path).unwrap();

    let mut zipper = zip::ZipWriter::new(file);
    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o755);

    let extract_dir = extract_dir_from_cbz_path(cbz_path);
    let mut buffer = Vec::new();
    for entry in WalkDir::new(&extract_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let entry = entry.path();
        debug!("Add file: {:?}", entry);
        let file_name = entry.strip_prefix(&extract_dir).unwrap();
        let path_string = file_name
            .to_str()
            .to_owned()
            .expect("Path is not UTF-8 conformant");

        if entry.is_file() {
            zipper.start_file(path_string, options).unwrap();
            fs::File::open(entry)
                .unwrap()
                .read_to_end(&mut buffer)
                .unwrap();
            zipper.write_all(&buffer).unwrap();
            buffer.clear();
        } else if !file_name.as_os_str().is_empty() {
            zipper.add_directory(path_string, options).unwrap();
        }
    }

    zipper.finish().unwrap();
}

fn main() {
    pretty_env_logger::init();
    let args: Vec<_> = env::args().collect();

    let parent_path = if args.len() > 1 {
        let path_str = &args[1];
        let path = Path::new(path_str);
        if !path.exists() {
            error!("Path does not exist: {:?}", path);
            exit(1);
        }
        if !path.is_dir() {
            error!("Path is not a directory: {:?}", path);
            exit(1);
        }
        path
    } else {
        Path::new(".")
    };

    for cbz_file in parent_path.read_dir().expect("read dir call failed!") {
        if let Ok(cbz_file) = cbz_file {
            debug!("Got: {:?}", cbz_file.path());
            if !cbz_contains_convertable_images(&cbz_file.path()) {
                info!("Nothing to do for {:?}", cbz_file.path());
                continue;
            }
            if already_converted(&cbz_file.path()) {
                info!("Conversion already exists");
                continue;
            }

            info!("Converting {:?}", cbz_file.path());
            let work_unit = WorkUnit::new(&cbz_file.path());
            extract_cbz(&work_unit);
            convert_images(&work_unit);
            compress_cbz(&work_unit);
        }
    }
}
