use crate::records::accurite::AccuriteRecord;
use async_trait::async_trait;
use influxdb_line_protocol::builder::LineProtocolBuilder;
use std::process::Stdio;
use std::time::Duration;
use std::time::SystemTime;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct RtlRunner {
    pub records_tx: mpsc::Sender<Vec<u8>>,
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

            let timestamp_nanos = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos(); // u128

            let line_writeable = LineProtocolBuilder::new()
                .measurement("accurite_tower")
                .tag("model", &record.model)
                .tag("channel", &record.channel)
                .tag("id", format!("{}", record.id).as_str())
                .field("battery_ok", record.battery_ok)
                .field("temperature_c", record.temperature_c)
                .field("humidity", record.humidity as i64)
                .timestamp(timestamp_nanos as i64)
                .close_line();

            let out_buf = line_writeable.build();

            self.records_tx
                .send(out_buf)
                .await
                .map_err(|e| TaskError::msg(format!("Failed to send record: {}", e)))?;
            line.clear(); // Clear the buffer for the next line
        }

        // A task could run forever and never return
        println!("Task completed!");
        Ok(())
    }
}
