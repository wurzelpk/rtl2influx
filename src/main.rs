use rtl2influx::influx_sender::InfluxSender;
use rtl2influx::records::accurite::AccuriteRecord;
use rtl2influx::rtl_runner::RtlRunner;
use task_supervisor::{SupervisedTask, SupervisorBuilder};

#[tokio::main]
async fn main() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<AccuriteRecord>(20);

    // Build the supervisor with initial tasks
    let supervisor = SupervisorBuilder::default().build();

    // Run the supervisor and get the handle
    let handle = supervisor.run();

    println!("Adding Influx Sender task...");
    handle
        .add_task(
            "influx_sender",
            InfluxSender {
                records_rx: std::sync::Arc::new(tokio::sync::Mutex::new(_rx)),
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
