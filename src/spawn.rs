use std::path::Path;
use std::process::{Child, Command, Stdio};

use exn::{ErrorExt, Exn, ResultExt};
use tracing::{debug, error};

use crate::error::ErrorMessage;

/// Child process that gets killed on drop.
#[derive(Debug)]
pub struct ManagedChild {
    cmd: String,
    child: Option<Child>,
}

impl ManagedChild {
    /// Spawn a new [ManagedChild].
    pub fn spawn(mut cmd: Command) -> Result<ManagedChild, Exn<ErrorMessage>> {
        let cmd_str = format!("{cmd:?}");

        let err = || ErrorMessage::new(format!("Failed to spawn the process: {cmd_str}"));

        debug!("spawn process: {cmd_str}");

        let spawned = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn();
        match spawned {
            Ok(child) => Ok(ManagedChild::new(cmd_str, child)),
            Err(e) => Err(Exn::new(e).raise(err())),
        }
    }

    /// Try to wait on the child process without blocking.
    pub fn try_wait(&mut self) -> Result<bool, Exn<ErrorMessage>> {
        let err = || {
            let cmd = &self.cmd;
            ErrorMessage::new(format!("Error when waiting on a child process: '{cmd}'",))
        };

        let waited = self.child.as_mut().unwrap().try_wait().or_raise(err)?;
        Ok(waited.is_some())
    }

    /// Wait on the child process to finish.
    pub fn wait(self) -> Result<(), Exn<ErrorMessage>> {
        self.wait_with_output()?;
        Ok(())
    }

    /// Wait on the child process to finish.
    ///
    /// Returns an error when the sub-process indicates an error.
    pub fn wait_with_output(mut self) -> Result<std::process::Output, Exn<ErrorMessage>> {
        let child = self.child.take().unwrap();

        let err = || {
            let cmd = &self.cmd;
            ErrorMessage::new(format!("Error when waiting on a child process: '{cmd}'",))
        };

        let output = child.wait_with_output().or_raise(err)?;
        if !output.status.success() {
            let abnormal_exit = match output.stderr.try_into() {
                Ok(s) => {
                    let s: String = s;
                    let msg =
                        format!("Process exited with an error and the following stderr:\n{s}");
                    ErrorMessage::new(msg).raise()
                }
                Err(e) => {
                    let e = e.raise();
                    let msg = "Process had an error, but stderr can not be parsed";
                    e.raise(ErrorMessage::new(msg))
                }
            };
            let exn = abnormal_exit.raise(err());
            return Err(exn);
        }
        Ok(output)
    }

    /// Internal constructor for a [ManagedChild].
    fn new(cmd: String, child: Child) -> Self {
        Self {
            cmd,
            child: Some(child),
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            debug!("drop child process running: {}", self.cmd);
            // ignore errors
            if let Err(e) = child.kill() {
                error!("error killing child process: {e}");
            }

            // is this necessary?
            if let Err(e) = child.wait() {
                error!("error waiting for killed child process: {e}");
            }
        }
    }
}

/// Run a conversion by invoking `magick`.
pub fn convert_with_magick(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Magick;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([input_path.to_str().unwrap(), output_path.to_str().unwrap()]);
    ManagedChild::spawn(cmd)
}

/// Convert from Jpeg to Png using `magick`.
pub fn convert_jpeg_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    convert_with_magick(input_path, output_path)
}

/// Convert from Png to Jpeg using `magick`.
pub fn convert_png_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Magick;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        "-quality",
        "92",
        output_path.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Encode an Avif file using `cavif`.
pub fn encode_avif(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Cavif;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "--speed=3",
        "--threads=1",
        "--quality=88",
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Encode a Jxl file using `cjxl`.
pub fn encode_jxl(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Cjxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "--effort=9",
        "--num_threads=1",
        "--distance=0",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Encode a Webp file using `cwebp`.
pub fn encode_webp(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Cwebp;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "-q",
        "90",
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Decode a Webp file using `dwebp`.
pub fn decode_webp(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Dwebp;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Decode a Jxl file to Png using `djxl`.
pub fn decode_jxl_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Djxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    ManagedChild::spawn(cmd)
}

/// Decode a Jxl file to Jpeg using `djxl`.
pub fn decode_jxl_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Djxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    ManagedChild::spawn(cmd)
}

/// Decode an Avif file to Png using `avifdec`.
pub fn decode_avif_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Avifdec;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "--jobs",
        "1",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Decode an Avif file to Jpeg using `avifdec`.
pub fn decode_avif_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Avifdec;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "--jobs",
        "1",
        "--quality",
        "80",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Run `jxlinfo` on a Jxl file to extract metadata.
pub fn run_jxlinfo(image_path: &Path) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::Jxlinfo;

    let mut cmd = Command::new(TOOL.name());
    cmd.args(["-v", image_path.to_str().unwrap()]);
    ManagedChild::spawn(cmd)
}

/// Use `7z` to list all files inside an archive.
pub fn list_archive_files(archive: &Path) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::_7z;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "l",
        "-ba",  // undocumented switch to remove header lines
        "-slt", // use format that is easier to parse
        archive.to_str().unwrap(),
    ]);
    ManagedChild::spawn(cmd)
}

/// Use `7z` to extract an archive.
pub fn extract_zip(archive: &Path, destination: &Path) -> Result<ManagedChild, Exn<ErrorMessage>> {
    const TOOL: Tool = Tool::_7z;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "x",
        "-tzip", // undocumented switch to remove header lines
        archive.to_str().unwrap(),
        "-spe",
        format!("-o{}", destination.to_str().unwrap()).as_str(),
    ]);
    ManagedChild::spawn(cmd)
}

/// All external tools used that may be used during conversion.
#[derive(Debug, Clone, Copy)]
pub enum Tool {
    Magick,
    Cavif,
    Cjxl,
    Cwebp,
    Dwebp,
    Djxl,
    Avifdec,
    Jxlinfo,
    _7z,
}

impl Tool {
    /// Get the (linux) executable name for the tool in question.
    fn name(self) -> &'static str {
        match self {
            Tool::Magick => "magick",
            Tool::Cavif => "cavif",
            Tool::Cjxl => "cjxl",
            Tool::Cwebp => "cwebp",
            Tool::Dwebp => "dwebp",
            Tool::Djxl => "djxl",
            Tool::Avifdec => "avifdec",
            Tool::Jxlinfo => "jxlinfo",
            Tool::_7z => "7z",
        }
    }
}

impl std::fmt::Display for Tool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}
