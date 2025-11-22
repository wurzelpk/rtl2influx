use crate::records::accurite::AccuriteRecord;
use async_trait::async_trait;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::mpsc;

use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

#[derive(Clone)]
pub struct RtlRunner {
    pub records_tx: mpsc::Sender<AccuriteRecord>,
}

#[async_trait]
impl SupervisedTask for RtlRunner {
    async fn run(&mut self) -> Result<(), TaskError> {
        let mut child = Command::new("./fake_rtl.sh")
            // .arg("-l") // Add arguments as needed
            .stdout(Stdio::piped())
            .spawn()?;

        // 2. Get the stdout handle and wrap it in a BufReader.
        let stdout = child
            .stdout
            .take()
            .expect("Child process did not have a stdout handle");
        let mut reader = BufReader::new(stdout);

        // 3. Create a buffer to read lines into.
        let mut line = String::new();

        // 4. Loop and read lines until EOF.
        while reader.read_line(&mut line).await? != 0 {
            let record = serde_json::from_str::<AccuriteRecord>(&line)
                .map_err(|e| TaskError::msg(format!("Failed to parse JSON: {}", e)))?;
            self.records_tx
                .send(record)
                .await
                .map_err(|e| TaskError::msg(format!("Failed to send record: {}", e)))?;
            line.clear(); // Clear the buffer for the next line
        }

        // A task could run forever and never return
        println!("Task completed!");
        Ok(())
    }
}
