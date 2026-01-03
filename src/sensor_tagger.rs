use async_trait::async_trait;
use influxdb2::models::DataPoint;
use influxdb2::models::data_point::DataPointBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::records::accurite::AccuriteRecord;

#[derive(Clone, serde::Deserialize)]
pub struct SensorTypeConfig {
    pub instances: HashMap<String, TagSet>,
}

#[derive(Clone, serde::Deserialize)]
pub struct TagSet {
    pub tags: HashMap<String, String>,
}

#[derive(Clone)]
pub struct SensorTagger {
    pub raw_rx: Arc<Mutex<mpsc::Receiver<String>>>,
    pub tagged_tx: mpsc::Sender<DataPointBuilder>,
    pub config: HashMap<String, SensorTypeConfig>,
}

#[async_trait]
impl SupervisedTask for SensorTagger {
    async fn run(&mut self) -> Result<(), TaskError> {
        let mut rx = self.raw_rx.lock().await;
        loop {
            match rx.recv().await {
                Some(json) => match self.decode_record(&json).await {
                    Some(record) => {
                        self.tagged_tx.send(record).await.map_err(|e| {
                            TaskError::msg(format!("Failed to send tagged record: {}", e))
                        })?;
                    }
                    None => {
                        println!("Failed to decode record: {}", json);
                    }
                },
                None => {
                    println!("Raw receiver channel closed.");
                    break;
                }
            }
        }
        Ok(())
    }
}

impl SensorTagger {
    async fn decode_record(&self, json: &str) -> Option<DataPointBuilder> {
        if json.contains("Acurite-Tower") {
            return self
                .decode_acurite_record(json, self.config.get("acutwr"))
                .await;
        } else {
            println!("Unknown sensor type in record: {}", json);
        }
        None
    }

    async fn decode_acurite_record(
        &self,
        json: &str,
        type_config: Option<&SensorTypeConfig>,
    ) -> Option<DataPointBuilder> {
        let record: AccuriteRecord = serde_json::from_str(json).ok()?;

        let timestamp_nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos(); // u128

        let mut point = DataPoint::builder("acurite_tower")
            .tag("model", &record.model)
            // .tag("channel", &record.channel) // Some sensors have flaky channel dip switches
            .tag("id", format!("{}", record.id).as_str())
            .field("battery_low", if record.battery_ok {0} else {1})
            .field("temp_c", record.temperature_c)
            .field("humidity", record.humidity as i64)
            .timestamp(timestamp_nanos as i64);

        if let Some(cfg) = type_config {
            if let Some(tagset) = cfg.instances.get(&record.id.to_string()) {
                for (k, v) in &tagset.tags {
                    point = point.tag(k, v);
                }
            }
        }

        Some(point)
    }
}
