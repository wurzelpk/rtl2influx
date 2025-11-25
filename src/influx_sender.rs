use async_trait::async_trait;
use futures::prelude::*;
use influxdb2::Client;
use influxdb2::models::DataPoint;
use influxdb2::models::data_point::DataPointBuilder;
use std::sync::Arc;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct InfluxSender {
    pub records_rx: Arc<Mutex<mpsc::Receiver<DataPointBuilder>>>,
}

#[async_trait]
impl SupervisedTask for InfluxSender {
    async fn run(&mut self) -> Result<(), TaskError> {
        let mut rx = self.records_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(record) => {
                    let point = record.build()?;
                    println!("Received record: {:?}", point);
                    // Here you would add code to send the record to InfluxDB
                    send_line(point).await.map_err(|e| {
                        TaskError::msg(format!("Failed to send to InfluxDB: {}", e))
                    })?;
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

async fn send_line(point: DataPoint) -> Result<(), Box<dyn std::error::Error>> {
    let host = "http://localhost:8086";
    let org = "pks";
    let token = std::env::var("INFLUX_TOKEN").unwrap();
    println!("Using InfluxDB token: {}", token);
    let bucket = "devel";
    let client = Client::new(host, org, token);

    let points = vec![point];

    println!("Sending points to InfluxDB...");
    client.write(bucket, stream::iter(points)).await?;
    println!("Points sent successfully.");
    Ok(())
}
