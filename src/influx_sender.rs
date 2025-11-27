use async_trait::async_trait;
use futures::prelude::*;
use influxdb2::Client;
use influxdb2::models::DataPoint;
use influxdb2::models::data_point::DataPointBuilder;
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

#[derive(Clone, serde::Deserialize)]
pub struct UploadConfig {
    pub max_events: usize,
    pub flush_interval: Duration,
}

#[derive(Clone)]
pub struct InfluxSender {
    pub influx_config: InfluxConfig,
    pub upload_config: UploadConfig,
    pub records_rx: Arc<Mutex<mpsc::Receiver<DataPointBuilder>>>,
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
        println!("Uploading {} points to InfluxDB...", points.len());
        if true {
            for p in &points {
                println!("Point: {:?}", p);
            }
        }
        client
            .write(&self.influx_config.bucket, stream::iter(points))
            .await
            .map_err(|e| TaskError::msg(format!("Failed to write to InfluxDB: {}", e)))?;
        println!("Upload complete.");
        Ok(())
    }
}

//             match rx.recv().await {
//                 Some(record) => {
//                     let point = record.build()?;
//                     println!("Received record: {:?}", point);
//                     // Here you would add code to send the record to InfluxDB
//                     send_line(point).await.map_err(|e| {
//                         TaskError::msg(format!("Failed to send to InfluxDB: {}", e))
//                     })?;
//                 }
//                 None => {
//                     println!("Channel closed, stopping InfluxSender task.");
//                     break;
//                 }
//             }
//         }
//         Ok(())
//     }
// }

// async fn send_line(point: DataPoint) -> Result<(), Box<dyn std::error::Error>> {
//     let host = "http://localhost:8086";
//     let org = "pks";
//     let token = std::env::var("INFLUX_TOKEN").unwrap();
//     println!("Using InfluxDB token: {}", token);
//     let bucket = "devel";
//     let client = Client::new(host, org, token);

//     let points = vec![point];

//     println!("Sending points to InfluxDB...");
//     client.write(bucket, stream::iter(points)).await?;
//     println!("Points sent successfully.");
//     Ok(())
// }
