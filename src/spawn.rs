use std::path::Path;
use std::process::{Child, Command, Stdio};

use exn::{ErrorExt, ResultExt, bail};
use tracing::{debug, error};

use crate::ErrorMessage;

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

/// Child process that gets killed on drop
#[derive(Debug)]
pub struct ManagedChild {
    cmd: String,
    child: Option<Child>,
}

impl ManagedChild {
    fn new(cmd: String, child: Child) -> Self {
        Self {
            cmd,
            child: Some(child),
        }
    }

    pub fn try_wait(&mut self) -> exn::Result<bool, ErrorMessage> {
        let err = || {
            let msg = format!("Error when waiting on a child process: '{}'", self.cmd);
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let waited = self.child.as_mut().unwrap().try_wait().or_raise(err)?;
        Ok(waited.is_some())
    }

    pub fn wait(self) -> exn::Result<(), ErrorMessage> {
        self.wait_with_output()?;
        Ok(())
    }

    pub fn wait_with_output(mut self) -> exn::Result<std::process::Output, ErrorMessage> {
        let child = self.child.take().unwrap();

        let err = || {
            let msg = format!(
                "Error when waiting on a child process running '{}'",
                self.cmd
            );
            debug!("{msg}");
            ErrorMessage(msg)
        };

        let output = child.wait_with_output().or_raise(err)?;
        if !output.status.success() {
            let abnormal_exit = match output.stderr.try_into() {
                Ok(s) => {
                    let s: String = s;
                    let msg =
                        format!("Process exited with an error and the following stderr:\n{s}");
                    ErrorMessage(msg).raise()
                }
                Err(e) => {
                    let msg = "Process had an error, but stderr can not be parsed".to_string();
                    debug!("{msg}");
                    e.raise().raise(ErrorMessage(msg))
                }
            };
            bail!(abnormal_exit.raise(err()));
        }
        Ok(output)
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

fn spawn(mut cmd: Command) -> exn::Result<ManagedChild, ErrorMessage> {
    let cmd_str = format!("{cmd:?}");

    let err = || {
        let msg = format!("Failed to spawn the process: {cmd_str}");
        debug!("{msg}");
        ErrorMessage(msg)
    };

    debug!("spawn process: {cmd_str}");

    let spawned = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn();
    match spawned {
        Ok(child) => Ok(ManagedChild::new(cmd_str, child)),
        Err(e) => Err(exn::Exn::new(e).raise(err())),
    }
}

pub fn convert_with_magick(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Magick;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([input_path.to_str().unwrap(), output_path.to_str().unwrap()]);
    spawn(cmd)
}

pub fn convert_jpeg_to_png(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    convert_with_magick(input_path, output_path)
}

pub fn convert_png_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Magick;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        "-quality",
        "92",
        output_path.to_str().unwrap(),
    ]);
    spawn(cmd)
}

pub fn encode_avif(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
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
    spawn(cmd)
}

pub fn encode_jxl(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Cjxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "--effort=9",
        "--num_threads=1",
        "--distance=0",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    spawn(cmd)
}

pub fn encode_webp(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Cwebp;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "-q",
        "90",
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    spawn(cmd)
}

pub fn decode_webp(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Dwebp;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    spawn(cmd)
}

pub fn decode_jxl_to_png(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Djxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    spawn(cmd)
}

pub fn decode_jxl_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Djxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    spawn(cmd)
}

pub fn decode_avif_to_png(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Avifdec;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "--jobs",
        "1",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    spawn(cmd)
}

pub fn decode_avif_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> exn::Result<ManagedChild, ErrorMessage> {
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
    spawn(cmd)
}

pub fn run_jxlinfo(image_path: &Path) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::Jxlinfo;

    let mut cmd = Command::new(TOOL.name());
    cmd.args(["-v", image_path.to_str().unwrap()]);
    spawn(cmd)
}

pub fn list_archive_files(archive: &Path) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::_7z;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "l",
        "-ba",  // undocumented switch to remove header lines
        "-slt", // use format that is easier to parse
        archive.to_str().unwrap(),
    ]);
    spawn(cmd)
}

pub fn extract_zip(archive: &Path, destination: &Path) -> exn::Result<ManagedChild, ErrorMessage> {
    const TOOL: Tool = Tool::_7z;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "x",
        "-tzip", // undocumented switch to remove header lines
        archive.to_str().unwrap(),
        "-spe",
        format!("-o{}", destination.to_str().unwrap()).as_str(),
    ]);
    spawn(cmd)
}
