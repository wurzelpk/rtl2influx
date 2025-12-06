use async_trait::async_trait;
use futures::prelude::*;
use influxdb2::Client;
use influxdb2::models::data_point::DataPointBuilder;
use serde_with::{DurationSeconds, serde_as};
use std::sync::Arc;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Clone, serde::Deserialize)]
pub struct InfluxConfig {
    pub host: String,
    pub org: String,
    pub bucket: String,
    pub token: String,
}

#[serde_as]
#[derive(Clone, serde::Deserialize)]
pub struct UploadConfig {
    pub max_events: usize,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub flush_interval: Duration,
}

#[derive(Clone)]
pub struct InfluxSenderConfig {
    pub influx_config: InfluxConfig,
    pub upload_config: UploadConfig,
    pub records_rx: Arc<Mutex<mpsc::Receiver<DataPointBuilder>>>,
}

#[derive(Clone)]
pub struct InfluxSenderStatus {
    pub events_uploaded: Arc<Mutex<usize>>,
    pub last_upload_time: Arc<Mutex<std::time::Instant>>,
}

#[derive(Clone)]
pub struct InfluxSender {
    influx_config: InfluxConfig,
    upload_config: UploadConfig,
    records_rx: Arc<Mutex<mpsc::Receiver<DataPointBuilder>>>,
    events_uploaded: Arc<Mutex<usize>>,
    last_upload_time: Arc<Mutex<std::time::Instant>>,
}

#[async_trait]
impl SupervisedTask for InfluxSender {
    async fn run(&mut self) -> Result<(), TaskError> {
        loop {
            match self.do_upload_pass().await {
                Ok(_) => (),
                Err(e) => {
                    println!("Error during InfluxDB upload pass: {}", e);
                }
            }

            tokio::time::sleep(self.upload_config.flush_interval).await;
        }
    }
}

impl InfluxSender {
    pub fn new(config: InfluxSenderConfig, status: InfluxSenderStatus) -> Self {
        InfluxSender {
            influx_config: config.influx_config,
            upload_config: config.upload_config,
            records_rx: config.records_rx,
            events_uploaded: status.events_uploaded,
            last_upload_time: status.last_upload_time,
        }
    }
    async fn do_upload_pass(&mut self) -> Result<(), TaskError> {
        let client = Client::new(
            &self.influx_config.host,
            &self.influx_config.org,
            &self.influx_config.token,
        );
        let mut rx = self.records_rx.lock().await;
        let mut points = vec![];
        loop {
            match rx.try_recv() {
                Ok(record) => {
                    let point = record.build().map_err(|e| {
                        TaskError::msg(format!("Failed to build DataPoint from builder: {}", e))
                    })?;
                    points.push(point);
                    if points.len() >= self.upload_config.max_events {
                        break;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    // No more records available right now
                    break;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return Err(TaskError::msg(
                        "Records channel disconnected, stopping InfluxSender task.",
                    ));
                }
            }
        }
        if points.is_empty() {
            println!("No points to upload in this pass.");
            return Ok(());
        }
        let point_count = points.len();
        println!("Uploading {} points to InfluxDB...", point_count);
        if true {
            for p in &points {
                println!("Point: {:?}", p);
            }
        }
        client
            .write(&self.influx_config.bucket, stream::iter(points))
            .await
            .map_err(|e| TaskError::msg(format!("Failed to write to InfluxDB: {}", e)))?;

        *(self.events_uploaded.lock().await) += point_count;
        *(self.last_upload_time.lock().await) = std::time::Instant::now();
        println!("Upload complete.");
        Ok(())
    }
}
