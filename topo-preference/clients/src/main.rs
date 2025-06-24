mod weather; 
use clients::base::*; 
use clients::client::*;
use crate::weather::*;
use std::{sync::{Arc, Mutex, mpsc}, thread};
use anyhow::Result;
use futures::executor::block_on;

// Parallel queue of clients that implement update  
pub fn run_clients(
    mut updaters: Vec<Box<dyn Updater + Send>>, 
    states: &[&str], 
    limit: usize, 
    max_threads: usize 
) -> Vec<anyhow::Result<()>> 
{
    // Enqueue clients without passing capacity 
    let (tx, rx) = mpsc::channel::<Box<dyn Updater + Send>>();
    for u in updaters.drain(..) {
        tx.send(u).unwrap();
    }
    drop(tx);

    // Spawn max number of threads (Or less)
    let mut handles = Vec::with_capacity(max_threads);
    for _ in 0..max_threads {
        let rx_c = Arc::clone(&rx);
        let states = states.to_owned();
        let handle = thread::spawn(move || {
            while let Ok(mut updater) = rx_c.lock().unwrap().recv() {
                let result: Result<()> = block_on(async {
                    updater.update(&states, limit).await
                });

                match result {
                    Ok(()) => println!("[thread {:?}] succeeded", thread::current().id()),
                    Err(e) => eprintln!("[thread {:?}] failed: {}", thread::current().id(), e)
                }
            }
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for handle in handles {
        results.extend(handle.join().expect("worker thread panicked"));
    }

    return results;
}
// Calls all clients in parallel to query 
#[tokio::main]
async fn main() -> Result<()> {
    // Workflow for all clients 
    // Create clients here 
    // Push clients into Array of updater objects 
    let mut clients: Vec<Box<dyn Updater + Send>> = vec![
        Box::new(WeatherClient::new(":memory:")?),
    ]; 

    // Run parallel api calls 
    let _ = run_clients(clients, &ALL_STATES, 10, 5);

    Ok(())
}
