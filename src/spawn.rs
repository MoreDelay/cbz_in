use std::path::Path;
use std::process::{Child, Command, Stdio};

use thiserror::Error;
use tracing::trace;

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

#[derive(Debug, Error)]
pub enum AbnormalExit {
    #[error("Could not parse error output")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("Process printed error output:\n{0}")]
    StdErr(String),
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("Could not spawn a process for the tool '{0}'")]
    Spawn(Tool, #[source] std::io::Error),
    #[error("A process for the tool '{0}' exited abnormally")]
    AbnormalExit(Tool, #[source] AbnormalExit),
    #[error("Could not wait on a child process for the tool '{0}'")]
    Wait(Tool, #[source] std::io::Error),
}

/// Child process that gets killed on drop
#[derive(Debug)]
pub struct ManagedChild {
    child: Option<Child>,
    tool: Tool,
}

impl ManagedChild {
    fn new(child: Child, tool: Tool) -> Self {
        Self {
            child: Some(child),
            tool,
        }
    }

    pub fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, ProcessError> {
        self.child
            .as_mut()
            .unwrap()
            .try_wait()
            .map_err(|e| ProcessError::Wait(self.tool, e))
    }

    fn into_inner(mut self) -> Child {
        self.child.take().unwrap()
    }

    pub fn wait_with_output(self) -> Result<std::process::Output, ProcessError> {
        let tool = self.tool;
        let output = self
            .into_inner()
            .wait_with_output()
            .map_err(|e| ProcessError::Wait(tool, e))?;
        if !output.status.success() {
            let abnormal_exit = match output.stderr.try_into() {
                Ok(s) => AbnormalExit::StdErr(s),
                Err(e) => AbnormalExit::InvalidUtf8(e),
            };
            return Err(ProcessError::AbnormalExit(tool, abnormal_exit));
        }
        Ok(output)
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
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
) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Magick;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([input_path.to_str().unwrap(), output_path.to_str().unwrap()]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn convert_png_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Magick;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        "-quality",
        "92",
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn encode_avif(input_path: &Path, output_path: &Path) -> Result<ManagedChild, ProcessError> {
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
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn encode_jxl(input_path: &Path, output_path: &Path) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Cjxl;

    let mut cmd = Command::new(TOOL.name());
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
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn encode_webp(input_path: &Path, output_path: &Path) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Cwebp;

    let mut cmd = Command::new(TOOL.name());
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
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn decode_webp(input_path: &Path, output_path: &Path) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Dwebp;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        "-o",
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn decode_jxl_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Djxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn decode_jxl_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Djxl;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        "--num_threads=1",
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn decode_avif_to_png(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Avifdec;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "--jobs",
        "1",
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn decode_avif_to_jpeg(
    input_path: &Path,
    output_path: &Path,
) -> Result<ManagedChild, ProcessError> {
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
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn run_jxlinfo(image_path: &Path) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::Jxlinfo;

    let mut cmd = Command::new(TOOL.name());
    cmd.args(["-v", image_path.to_str().unwrap()]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn list_archive_files(archive: &Path) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::_7z;

    let mut cmd = Command::new(TOOL.name());
    cmd.args([
        "l",
        "-ba",  // undocumented switch to remove header lines
        "-slt", // use format that is easier to parse
        archive.to_str().unwrap(),
    ]);
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}

pub fn extract_zip(archive: &Path, destination: &Path) -> Result<ManagedChild, ProcessError> {
    const TOOL: Tool = Tool::_7z;

    let mut cmd = Command::new(TOOL.name());
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
        .map_err(|e| ProcessError::Spawn(TOOL, e))
        .map(|c| ManagedChild::new(c, TOOL))
}
