use crate::records::accurite::AccuriteRecord;
use async_trait::async_trait;
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskError};
use tokio::sync::mpsc;

#[derive(Clone)]
struct MyTask {
    pub records_tx: mpsc::Sender<AccuriteRecord>,
}

#[async_trait]
impl SupervisedTask for MyTask {
    async fn run(&mut self) -> Result<(), TaskError> {
        for _ in 0..15 {
            println!("Task is running!");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        // A task could run forever and never return
        println!("Task completed!");
        Ok(())
    }
}
