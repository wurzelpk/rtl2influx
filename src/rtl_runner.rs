use crate::records::accurite::AccuriteRecord;
use async_trait::async_trait;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct RtlRunner {
    pub records_tx: mpsc::Sender<AccuriteRecord>,
}

#[async_trait]
impl SupervisedTask for RtlRunner {
    async fn run(&mut self) -> Result<(), TaskError> {
        for _ in 0..15 {
            println!("Sending Record...");
            let record = AccuriteRecord {
                time: "2025-11-21 01:26:21".to_string(),
                model: "Acurite-Tower".to_string(),
                id: 10956,
                channel: "A".to_string(),
                battery_ok: true,
                temperature_c: 22.7,
                humidity: 55,
                mic: "CHECKSUM".to_string(),
            };
            self.records_tx
                .send(record)
                .await
                .map_err(|e| TaskError::msg(format!("Failed to send record: {}", e)))?;
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        // A task could run forever and never return
        println!("Task completed!");
        Ok(())
    }
}
