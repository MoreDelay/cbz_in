use std::path::Path;
use std::process::{Child, Command, Stdio};

use crate::ConversionError::{self, *};

pub fn convert_jpeg_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<Child, ConversionError> {
    let mut command = Command::new("magick");
    command.args([input_path.to_str().unwrap(), output_path.to_str().unwrap()]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("magick".to_string()))?;
    Ok(child)
}

pub fn convert_png_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<Child, ConversionError> {
    let mut command = Command::new("magick");
    command.args([
        input_path.to_str().unwrap(),
        "-quality",
        "92",
        output_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("magick".to_string()))?;
    Ok(child)
}

pub fn encode_avif(input_path: &Path, output_path: &Path) -> Result<Child, ConversionError> {
    let mut command = Command::new("cavif");
    command.args([
        "--speed=3",
        "--threads=1",
        "--quality=88",
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("cavif".to_string()))?;
    Ok(child)
}

pub fn encode_jxl(input_path: &Path, output_path: &Path) -> Result<Child, ConversionError> {
    let mut command = Command::new("cjxl");
    command.args([
        "--effort=9",
        "--num_threads=1",
        "--distance=0",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("cjxl".to_string()))?;
    Ok(child)
}

pub fn encode_webp(input_path: &Path, output_path: &Path) -> Result<Child, ConversionError> {
    let mut command = Command::new("cwebp");
    command.args([
        "-q",
        "90",
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("cwebp".to_string()))?;
    Ok(child)
}

pub fn decode_webp(input_path: &Path, output_path: &Path) -> Result<Child, ConversionError> {
    let mut command = Command::new("dwebp");
    command.args([
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("dwebp".to_string()))?;
    Ok(child)
}

pub fn decode_jxl_to_png(input_path: &Path, output_path: &Path) -> Result<Child, ConversionError> {
    let mut command = Command::new("djxl");
    command.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("djxl".to_string()))?;
    Ok(child)
}

pub fn decode_jxl_to_jpeg(input_path: &Path, output_path: &Path) -> Result<Child, ConversionError> {
    let mut command = Command::new("djxl");
    command.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("djxl".to_string()))?;
    Ok(child)
}

pub fn decode_avif_to_png(input_path: &Path, output_path: &Path) -> Result<Child, ConversionError> {
    let mut command = Command::new("avifdec");
    command.args([
        "--jobs",
        "1",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("avifdec".to_string()))?;
    Ok(child)
}

pub fn decode_avif_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<Child, ConversionError> {
    let mut command = Command::new("avifdec");
    command.args([
        "--jobs",
        "1",
        "--quality",
        "80",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| SpawnFailure("avifdec".to_string()))?;
    Ok(child)
}
