use async_trait::async_trait;
use std::process::Stdio;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct RtlRunner {
    pub records_tx: mpsc::Sender<String>,
}

#[async_trait]
impl SupervisedTask for RtlRunner {
    async fn run(&mut self) -> Result<(), TaskError> {
        let mut child = Command::new("./fake_rtl.sh")
            // .arg("-l") // Add arguments as needed
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
            }
            line.clear();
        }

        let status = child.wait().await?;
        println!("rtl_runner: EOF.  Process exited with status: {}", status);
        Ok(())
    }
}
