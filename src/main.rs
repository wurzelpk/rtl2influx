use config::Config;
use influxdb2::models::data_point::DataPointBuilder;
use rtl2influx::influx_sender::InfluxConfig;
use rtl2influx::influx_sender::InfluxSender;
use rtl2influx::influx_sender::UploadConfig;
use rtl2influx::rtl_runner::RtlRunner;
use rtl2influx::sensor_tagger::SensorTagger;
use task_supervisor::SupervisorBuilder;

#[derive(serde::Deserialize)]
struct AppConfig {
    pub node: String,
    pub influx: InfluxConfig,
    pub upload: Option<UploadConfig>,
}

#[tokio::main]
async fn main() {
    let (tx0, rx0) = tokio::sync::mpsc::channel::<DataPointBuilder>(20);
    let (tx1, rx1) = tokio::sync::mpsc::channel::<DataPointBuilder>(20);

    let settings = Config::builder()
        .add_source(config::File::with_name("test_conf"))
        .add_source(config::Environment::with_prefix("RTL2INFLUX"))
        .build()
        .unwrap();

    let config: AppConfig = settings.try_deserialize().unwrap();

    // Build the supervisor with initial tasks
    let supervisor = SupervisorBuilder::default().build();

    // Run the supervisor and get the handle
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
            },
        )
        .unwrap();

    println!("Adding RTL Runner task...");
    handle
        .add_task(
            "rtl_runner",
            RtlRunner {
                records_tx: tx0.clone(),
            },
        )
        .unwrap();

    println!("Awaiting task completion...");
    handle.wait().await.unwrap();

    println!("Task has finished.");
}
