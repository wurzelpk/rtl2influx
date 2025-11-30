use std::collections::HashMap;

use config::Config;
use influxdb2::models::data_point::DataPointBuilder;
use rtl2influx::influx_sender::InfluxConfig;
use rtl2influx::influx_sender::InfluxSender;
use rtl2influx::influx_sender::UploadConfig;
use rtl2influx::rtl_runner::RtlRunner;
use rtl2influx::rtl_runner::RtlRunnerConfig;
use rtl2influx::sensor_tagger::SensorTagger;
use rtl2influx::sensor_tagger::SensorTypeConfig;
use task_supervisor::SupervisorBuilder;

#[derive(serde::Deserialize)]
struct AppConfig {
    pub node: String,
    pub rtl_runner: RtlRunnerConfig,
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

    let config: AppConfig = settings.try_deserialize().unwrap();

    let supervisor = SupervisorBuilder::default().build();
    let handle = supervisor.run();

    println!("Adding Influx Sender task...");
    handle
        .add_task(
            "influx_sender",
            InfluxSender {
                records_rx: std::sync::Arc::new(tokio::sync::Mutex::new(rx1)),
                influx_config: config.influx,
                upload_config: config.upload.unwrap_or(UploadConfig {
                    max_events: 100,
                    flush_interval: std::time::Duration::from_secs(15),
                }),
            },
        )
        .unwrap();

    println!("Adding Sensor Tagger task...");
    handle
        .add_task(
            "sensor_tagger",
            SensorTagger {
                raw_rx: std::sync::Arc::new(tokio::sync::Mutex::new(rx0)),
                tagged_tx: tx1.clone(),
                config: config.sensors,
            },
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

    println!("Awaiting task completion...");
    handle.wait().await.unwrap();

    println!("Task has finished.");
}
