use async_trait::async_trait;
use influxdb2::models::data_point::DataPointBuilder;
use std::sync::Arc;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct SensorTagger {
    pub raw_rx: Arc<Mutex<mpsc::Receiver<DataPointBuilder>>>,
    pub tagged_tx: mpsc::Sender<DataPointBuilder>,
}

#[async_trait]
impl SupervisedTask for SensorTagger {
    async fn run(&mut self) -> Result<(), TaskError> {
        let mut rx = self.raw_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(mut record) => {
                    // Here you would add your tagging logic
                    record = record.tag("borkiness", "high");

                    self.tagged_tx.send(record).await.map_err(|e| {
                        TaskError::msg(format!("Failed to send tagged record: {}", e))
                    })?;
                }
                None => {
                    println!("Raw receiver channel closed.");
                    break;
                }
            }
        }
        Ok(())
    }
}
