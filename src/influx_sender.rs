use crate::records::accurite::AccuriteRecord;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct InfluxSender {
    pub records_rx: Arc<Mutex<mpsc::Receiver<AccuriteRecord>>>,
}

#[async_trait]
impl SupervisedTask for InfluxSender {
    async fn run(&mut self) -> Result<(), TaskError> {
        let mut rx = self.records_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(record) => {
                    println!("Received record: {:?}", record);
                    // Here you would add code to send the record to InfluxDB
                }
                None => {
                    println!("Channel closed, stopping InfluxSender task.");
                    break;
                }
            }
        }
        Ok(())
    }
}
