use config::Config;
use influxdb2::models::data_point::DataPointBuilder;
use rtl2influx::influx_sender::InfluxSender;
use rtl2influx::rtl_runner::RtlRunner;
use task_supervisor::SupervisorBuilder;

#[tokio::main]
async fn main() {
    let (tx, rx) = tokio::sync::mpsc::channel::<DataPointBuilder>(20);

    let settings = Config::builder()
        .add_source(config::File::with_name("test_conf"))
        .add_source(config::Environment::with_prefix("RTL2INFLUX"))
        .build()
        .unwrap();

    println!("Settings: {:?}", settings);

    // Build the supervisor with initial tasks
    let supervisor = SupervisorBuilder::default().build();

    // Run the supervisor and get the handle
    let handle = supervisor.run();

    println!("Adding Influx Sender task...");
    handle
        .add_task(
            "influx_sender",
            InfluxSender {
                records_rx: std::sync::Arc::new(tokio::sync::Mutex::new(rx)),
            },
        )
        .unwrap();

    println!("Adding RTL Runner task...");
    handle
        .add_task(
            "rtl_runner",
            RtlRunner {
                records_tx: tx.clone(),
            },
        )
        .unwrap();

    println!("Awaiting task completion...");
    handle.wait().await.unwrap();

    println!("Task has finished.");
}
