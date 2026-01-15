use std::path::Path;
use std::process::{Child, Command, Stdio};

use thiserror::Error;
use tracing::trace;

#[derive(Debug, Error)]
pub enum SpawnError {
    #[error("Could not spawn magick process: {0}")]
    Magick(String, #[source] std::io::Error),
    #[error("Could not spawn cavif process: {0}")]
    Cavif(String, #[source] std::io::Error),
    #[error("Could not spawn cjxl process: {0}")]
    Cjxl(String, #[source] std::io::Error),
    #[error("Could not spawn cwebp process: {0}")]
    Cwebp(String, #[source] std::io::Error),
    #[error("Could not spawn dwebp process: {0}")]
    Dwebp(String, #[source] std::io::Error),
    #[error("Could not spawn djxl process: {0}")]
    Djxl(String, #[source] std::io::Error),
    #[error("Could not spawn avifdec process: {0}")]
    Avifdec(String, #[source] std::io::Error),
    #[error("Could not spawn jxlinfo process: {0}")]
    Jxlinfo(String, #[source] std::io::Error),
    #[error("Could not spawn 7z process: {0}")]
    E7z(String, #[source] std::io::Error),
}

/// Child process that gets killed on drop
#[derive(Debug)]
pub struct ManagedChild(Option<Child>);

impl ManagedChild {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    pub fn into_inner(mut self) -> Child {
        self.0.take().unwrap()
    }
}

impl std::ops::Deref for ManagedChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for ManagedChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            trace!("drop {child:?}");
            // ignore errors
            let _ = child.kill();
            let _ = child.wait(); // is this necessary?
        }
    }
}

pub fn convert_jpeg_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("magick");
    cmd.args([input_path.to_str().unwrap(), output_path.to_str().unwrap()]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Magick(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn convert_png_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("magick");
    cmd.args([
        input_path.to_str().unwrap(),
        "-quality",
        "92",
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Magick(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn encode_avif(input_path: &Path, output_path: &Path) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("cavif");
    cmd.args([
        "--speed=3",
        "--threads=1",
        "--quality=88",
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Cavif(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn encode_jxl(input_path: &Path, output_path: &Path) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("cjxl");
    cmd.args([
        "--effort=9",
        "--num_threads=1",
        "--distance=0",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Cjxl(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn encode_webp(input_path: &Path, output_path: &Path) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("cwebp");
    cmd.args([
        "-q",
        "90",
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Cwebp(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn decode_webp(input_path: &Path, output_path: &Path) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("dwebp");
    cmd.args([
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Dwebp(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn decode_jxl_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("djxl");
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Djxl(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn decode_jxl_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("djxl");
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Djxl(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn decode_avif_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("avifdec");
    cmd.args([
        "--jobs",
        "1",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Avifdec(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn decode_avif_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("avifdec");
    cmd.args([
        "--jobs",
        "1",
        "--quality",
        "80",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Avifdec(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn run_jxlinfo(image_path: &Path) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("jxlinfo");
    cmd.args(["-v", image_path.to_str().unwrap()]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::Jxlinfo(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn list_archive_files(archive: &Path) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("7z");
    cmd.args([
        "l",
        "-ba",  // undocumented switch to remove header lines
        "-slt", // use format that is easier to parse
        archive.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::E7z(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}

pub fn extract_zip(archive: &Path, destination: &Path) -> Result<ManagedChild, SpawnError> {
    let mut cmd = Command::new("7z");
    cmd.args([
        "x",
        "-tzip", // undocumented switch to remove header lines
        archive.to_str().unwrap(),
        "-spe",
        format!("-o{}", destination.to_str().unwrap()).as_str(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SpawnError::E7z(format!("{cmd:?}"), e))
        .map(ManagedChild::new)
}
