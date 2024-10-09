use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use tokio::time::sleep;

use crate::account_and_transaction::{
    process_transaction_from_arc, receive_transaction, Account, Transaction,
};

pub async fn process_transactions_using_mpsc(num_partitions: usize) {
    // Create channels for each partitioned
    // mpsc is the current in-memory message passing channel
    // it ti
    let mut partitioned_senders: Vec<Sender<Transaction>> = Vec::with_capacity(num_partitions);
    let mut partitioned_receivers = Vec::with_capacity(num_partitions);
    let mut partitioned_accounts: Vec<Arc<Mutex<HashMap<u16, Account>>>> =
        Vec::with_capacity(num_partitions);

    // This adds index ordered partitions to the partitioned_senders, partitioned_receivers, and partitioned_accounts vectors
    for _ in 0..num_partitions {
        let (tx, rx) = mpsc::channel(32);
        partitioned_senders.push(tx);
        partitioned_receivers.push(rx);
        partitioned_accounts.push(Arc::new(Mutex::new(HashMap::new())));
    }

    println!(
        "Senders: {:?}, receivers: {:?}, accounts: {:?}",
        partitioned_senders, partitioned_receivers, partitioned_accounts
    );

    // spawn a worker for each partitioned
    for (_, (partitioned_account, worker)) in partitioned_accounts
        .into_iter()
        .zip(partitioned_receivers.into_iter())
        .enumerate()
    {
        let partitioned_account = Arc::clone(&partitioned_account); // Clone the Arc for each task

        // create the task

        task::spawn(async move {
            let mut worker = worker;
            while let Some(tx) = worker.recv().await {
                println!("Worker {:?}, processing tx: {:?}", worker, tx);
                process_transaction_from_arc(tx, &partitioned_account).await;
            }
        });
    }

    // receive transations
    for _ in 0..20 {
        let tx = receive_transaction().await;

        // distribute the account to the correct send based on account_id
        let partitioned_index = (tx.account_id as usize) % num_partitions;
        partitioned_senders[partitioned_index]
            .send(tx)
            .await
            .unwrap();

        // sleep(Duration::from_millis(500)).await;
    }
}
