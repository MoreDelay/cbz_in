use std::env;
use std::fs;
use std::io::BufReader;
use std::path::Path;
use std::process::exit;

use log::{debug, error, info};
use zip;

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

            info!("Converting {:?}", cbz_file.path());
            // Todo:
            // extract_cbz();
            // convert_images();
            // compress_cbz();
        }
    }
}
