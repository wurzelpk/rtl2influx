use std::collections::HashMap;

use config::Config;
use influxdb2::models::data_point::DataPointBuilder;
use rtl2influx::influx_sender::InfluxConfig;
use rtl2influx::influx_sender::InfluxSender;
use rtl2influx::influx_sender::InfluxSenderConfig;
use rtl2influx::influx_sender::InfluxSenderStatus;
use rtl2influx::influx_sender::UploadConfig;
use rtl2influx::rtl_runner::RtlRunner;
use rtl2influx::rtl_runner::RtlRunnerConfig;
use rtl2influx::sensor_tagger::SensorTagger;
use rtl2influx::sensor_tagger::SensorTypeConfig;
use serde_with::{DurationSeconds, serde_as};
use task_supervisor::SupervisorBuilder;

#[serde_as]
#[derive(Clone, serde::Deserialize)]
struct WatchdogConfig {
    #[serde_as(as = "DurationSeconds<u64>")]
    pub interval: std::time::Duration,
}

#[derive(serde::Deserialize)]
struct AppConfig {
    #[allow(unused)]
    pub node: String,
    pub rtl_runner: RtlRunnerConfig,
    pub watchdog: Option<WatchdogConfig>,
    pub influx: InfluxConfig,
    pub upload: Option<UploadConfig>,
    pub sensors: HashMap<String, SensorTypeConfig>,
}

#[tokio::main]
async fn main() {
    let mut argv = std::env::args();
    let _ = argv.next();
    let config_file_name = argv.next().expect("Usage: rtl2influx <config_file>");

    // Raw JSON strings flow from RTL Runner -> Sensor Tagger
    let (tx0, rx0) = tokio::sync::mpsc::channel::<String>(20);
    // Tagged DataPointBuilders flow from Sensor Tagger -> Influx Sender
    let (tx1, rx1) = tokio::sync::mpsc::channel::<DataPointBuilder>(20);

    let settings = Config::builder()
        .add_source(config::File::with_name(config_file_name.as_str()))
        .add_source(config::Environment::with_prefix("RTL2INFLUX"))
        .build()
        .unwrap();

    let mut config: AppConfig = settings.try_deserialize().unwrap();

    if config.influx.token.is_empty() {
        config.influx.token = std::env::var("INFLUX_TOKEN").unwrap_or_else(|_| {
            panic!("InfluxDB token is required but not provided.");
        });
    }
    let supervisor = SupervisorBuilder::default().build();
    let handle = supervisor.run();

    let status = InfluxSenderStatus {
        events_uploaded: std::sync::Arc::new(tokio::sync::Mutex::new(0)),
        last_upload_time: std::sync::Arc::new(tokio::sync::Mutex::new(std::time::Instant::now())),
    };

    println!("Adding Influx Sender task...");
    handle
        .add_task(
            "influx_sender",
            InfluxSender::new(
                InfluxSenderConfig {
                    influx_config: config.influx,
                    upload_config: config.upload.unwrap_or(UploadConfig {
                        max_events: 100,
                        flush_interval: std::time::Duration::from_secs(15),
                    }),
                    records_rx: std::sync::Arc::new(tokio::sync::Mutex::new(rx1)),
                },
                status.clone(),
            ),
        )
        .unwrap();

    println!("Adding Sensor Tagger task...");
    handle
        .add_task(
            "sensor_tagger",
            SensorTagger::new(
                std::sync::Arc::new(tokio::sync::Mutex::new(rx0)),
                tx1.clone(),
                config.sensors,
            ),
        )
        .unwrap();

    println!("Adding RTL Runner task...");
    handle
        .add_task(
            "rtl_runner",
            RtlRunner {
                records_tx: tx0.clone(),
                config: config.rtl_runner,
            },
        )
        .unwrap();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        match handle.get_all_task_statuses().await {
            Ok(statuses) => {
                println!(
                    "Total events uploaded: {}",
                    *(status.events_uploaded.lock().await)
                );
                for (name, status) in statuses {
                    println!("  {}: {:?}", name, status);
                }
            }
            Err(e) => {
                println!("Error getting all task statuses: {}", e);
                break;
            }
        }

        if let Some(watchdog_config) = &config.watchdog {
            let last_upload_time = *(status.last_upload_time.lock().await);
            if last_upload_time.elapsed() > watchdog_config.interval {
                println!(
                    "Watchdog: No uploads in the last {:?}, exiting.",
                    watchdog_config.interval
                );
                break;
            }
        }
    }

    println!("Shutting down...");
    handle.shutdown().expect("Failed to shutdown supervisor");
    handle.wait().await.expect("Failed to wait for supervisor");
    println!("Task has finished.");
}
