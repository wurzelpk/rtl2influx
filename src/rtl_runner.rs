use async_trait::async_trait;
use std::process::Stdio;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

#[derive(Clone, serde::Deserialize)]
pub struct RtlRunnerConfig {
    path: String,
    args: Vec<String>,
}

#[derive(Clone)]
pub struct RtlRunner {
    pub records_tx: mpsc::Sender<String>,
    pub config: RtlRunnerConfig,
}

#[async_trait]
impl SupervisedTask for RtlRunner {
    async fn run(&mut self) -> Result<(), TaskError> {
        let mut child = Command::new(&self.config.path)
            .args(&self.config.args)
            .stdout(Stdio::piped())
            .spawn()?;

        let stdout = child
            .stdout
            .take()
            .expect("Child process did not have a stdout handle");
        let mut reader = BufReader::new(stdout);

        let mut line = String::new();

        while reader.read_line(&mut line).await? != 0 {
            let trimmed = line.trim();
            if trimmed.starts_with('{') && trimmed.ends_with('}') {
                self.records_tx
                    .send(trimmed.to_string())
                    .await
                    .map_err(|e| TaskError::msg(format!("Failed to send record: {}", e)))?;
            } else {
                println!("rtl_runner: discard: {}", trimmed);
            }
            line.clear();
        }

        let status = child.wait().await?;
        println!("rtl_runner: EOF.  Process exited with status: {}", status);
        Err(TaskError::msg("rtl_runner process exited unexpectedly"))
    }
}
