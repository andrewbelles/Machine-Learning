mod weather; 
mod crime;

use clients::base::*; 
//use clients::client::*;
use crate::weather::*;
use std::{sync::{Arc, Mutex, mpsc}, thread};
use anyhow::Result;
use futures::executor::block_on;

// Parallel queue of clients that implement update  
pub fn run_clients(
    mut updaters: Vec<Box<dyn Updater + Send>>, 
    states: &'static [&'static str], 
    limit: usize, 
    max_threads: usize 
) -> Vec<anyhow::Result<()>> 
{
    // Enqueue clients without passing capacity 
    let (tx, rx) = mpsc::channel::<Box<dyn Updater + Send>>();
    let rx = Arc::new(Mutex::new(rx));

    for u in updaters.drain(..) {
        tx.send(u).unwrap();
    }
    drop(tx);

    // Spawn max number of threads (Or less)
    let mut handles = Vec::with_capacity(max_threads);
    for _ in 0..max_threads {
        let rx_c = Arc::clone(&rx);
        let handle = thread::spawn(move || -> Vec<anyhow::Result<()>> {
            let mut local_results = Vec::new();
            while let Ok(mut updater) = rx_c.lock().unwrap().recv() {
                local_results.push(block_on(updater.update(&states, limit)));
            }
            local_results
        });
        handles.push(handle);
    }

    let mut results: Vec<anyhow::Result<()>> = Vec::new();
    for handle in handles {
        let thread_results = handle.join().expect("worker thread panicked");
        results.extend(thread_results);
    }
    return results;
}

// Calls all clients in parallel to query 
#[tokio::main]
async fn main() -> Result<()> {
    // Workflow for all clients 
    // Create clients here 
    // Push clients into Array of updater objects 
    let clients: Vec<Box<dyn Updater + Send>> = vec![
        Box::new(WeatherClient::new(":memory:")?),
    ]; 

    // Run parallel api calls 
    let _ = run_clients(clients, &ALL_STATES_ABBR, 10, 5);

    Ok(())
}
