use dashmap::DashMap;
use rand::Rng;
use redis::streams::StreamReadReply;
use redis::{cmd, Commands, RedisResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, MutexGuard, RwLock};
use tokio::task;
use tokio::time::sleep;

#[derive(Debug, Deserialize, Serialize)]
enum TransactionType {
    Deposit,
    Withdrawal,
}

#[derive(Debug, Deserialize, Serialize)]
struct Transaction {
    account_id: u16,
    transaction_type: TransactionType,
    amount: f64,
}

#[derive(Debug)]
struct Account {
    balance: f64,
}

impl Account {
    fn new() -> Self {
        Account { balance: 0.0 }
    }

    fn deposit(&mut self, amount: f64) {
        self.balance += amount;
    }

    fn withdrawal(&mut self, amount: f64) {
        self.balance -= amount;
    }
}

// A mock endpoint to simulate receiving transactions intermittently
async fn process_transaction_from_mutex_guard(
    tx: Transaction,
    mut account: MutexGuard<'_, Account>,
) {
    match tx.transaction_type {
        TransactionType::Deposit => account.deposit(tx.amount),
        TransactionType::Withdrawal => account.withdrawal(tx.amount),
    }

    println!(
        "PROCESSED {:?} for account {}: new balance is {}",
        tx.transaction_type, tx.account_id, account.balance
    );
}

// A endpoint to simulate processing transactions by a worker thread
async fn process_transaction_from_arc(
    tx: Transaction,
    accounts: &Arc<Mutex<HashMap<u16, Account>>>,
) {
    // lock the accounts while processing
    let mut accounts = accounts.lock().await;
    // Get or create the account
    let account = accounts.entry(tx.account_id).or_insert_with(Account::new);

    // Transaction validation step would occur here

    // And then the account-level step would occur here
    match tx.transaction_type {
        TransactionType::Deposit => account.deposit(tx.amount),
        TransactionType::Withdrawal => account.withdrawal(tx.amount),
    }

    println!(
        "PROCESSED {:?} for account {}: new balance is {}",
        tx.transaction_type, tx.account_id, account.balance
    );
}
// A mock endpoint to send randomized transactions
async fn receive_transaction() -> Transaction {
    let mut rng = rand::thread_rng();

    let transaction_type = if rng.gen_bool(0.5) {
        TransactionType::Deposit
    } else {
        TransactionType::Withdrawal
    };
    Transaction {
        account_id: rng.gen_range(1..100),
        transaction_type,
        amount: rng.gen_range(1.0..1000.0),
    }
}

pub async fn process_using_redis_consumer_groups(
    num_partitions: usize,
) -> Result<(), Box<dyn Error>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;

    let mut partitioned_account_groups: Vec<Arc<DashMap<u16, Arc<Mutex<Account>>>>> =
        Vec::with_capacity(num_partitions);

    // push a group of partition of accounts to the groups
    for _ in 0..num_partitions {
        partitioned_account_groups.push(Arc::new(DashMap::new()));
    }

    let client_arc = Arc::new(client);

    for partition_index in 0..num_partitions {
        let stream_name = format!("transactions_stream_{}", partition_index);
        let consumer_group_name = "transaction_group";
        let consumer_name = format!("consumer_{}", partition_index);

        {
            let mut con = client_arc.get_multiplexed_async_connection().await?;
            let _: () = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(&stream_name)
                .arg(consumer_group_name)
                .arg("$")
                .arg("MKSTREAM")
                .query_async(&mut con)
                .await
                .unwrap_or(());
        }

        // Create four workers per partition
        for worker_id in 0..4 {
            let stream_name = stream_name.clone();
            let consumer_name = format!("{}_worker_{}", consumer_name, worker_id);
            let client_clone = Arc::clone(&client_arc);

            let accounts: Arc<DashMap<u16, Arc<Mutex<Account>>>> =
                Arc::clone(&partitioned_account_groups[partition_index]);

            tokio::spawn(async move {
                let mut con = client_clone
                    .get_multiplexed_async_connection()
                    .await
                    .expect("Failed to open async connection");
                println!(
                    "pre loop, partition_index: {}, worker: {}",
                    partition_index, worker_id
                );
                loop {
                    // Read from Redis stream as part of the consumer group
                    let res: RedisResult<StreamReadReply> = cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&consumer_group_name)
                        .arg(&consumer_name)
                        .arg("BLOCK")
                        .arg(0) // Wait indefinitely for new messages
                        .arg("COUNT")
                        .arg(1) // Fetch one message at a time
                        .arg("STREAMS")
                        .arg(&stream_name)
                        .arg(">")
                        .query_async(&mut con)
                        .await; // Execute the command

                    match res {
                        Ok(reply) => {
                            for stream_key in reply.keys {
                                for message in stream_key.ids {
                                    let tx_data: String = match message.get::<String>("data") {
                                        Some(data) => data.to_string(),
                                        None => {
                                            return Err::<String, _>(
                                                "No tx data in stream".to_string(),
                                            )
                                        }
                                    };

                                    let tx: Transaction = serde_json::from_str(&tx_data).unwrap();

                                    // This WAS interesting
                                    // using the RwLock, I am limiting locking the partition to ONLY writes of new accounts
                                    // while also allowing reads to be concurrent
                                    // The problem with it was that it introduced a small change that between dropping the read and acquiring the write another worker could do the same
                                    // let account_mutex = {
                                    //     let read_accounts = accounts.read().await;
                                    //     if let Some(account_mutex) =
                                    //         read_accounts.get(&tx.account_id)
                                    //     {
                                    //         Arc::clone(account_mutex)
                                    //     } else {
                                    //         drop(read_accounts);
                                    //         let mut write_accounts = accounts.write().await;
                                    //         write_accounts
                                    //             .entry(tx.account_id)
                                    //             .or_insert_with(|| {
                                    //                 Arc::new(Mutex::new(Account::new()))
                                    //             })
                                    //             .clone()
                                    //     }
                                    // };

                                    // Dashmap solves the above problem
                                    let account_mutex = accounts
                                        .entry(tx.account_id)
                                        .or_insert_with(|| Arc::new(Mutex::new(Account::new())))
                                        .clone();

                                    // Lock the account
                                    let account = account_mutex.lock().await;

                                    // Process the transaction
                                    process_transaction_from_mutex_guard(tx, account).await;

                                    // Acknowledge the message to redis
                                    let _: () = redis::cmd("XACK")
                                        .arg(&stream_name)
                                        .arg(&consumer_group_name)
                                        .arg(&message.id)
                                        .query_async(&mut con)
                                        .await
                                        .unwrap();
                                }
                            }
                        }
                        Err(e) => {
                            return Err(format!("Error reading from Redis stream: {}", e));
                        }
                    }

                    println!("end of loop");
                }
            });
        }
    }

    let mut con = client_arc.get_multiplexed_async_connection().await?;

    // Receive transactions and send to the partitioned Redis stream
    for i in 0..20 {
        println!("should loop lots, loop: {}", i);
        let transaction = receive_transaction().await;

        println!("CREATED: {}", transaction.account_id);
        let tx_data = serde_json::to_string(&transaction)?;

        let partitioned_index = transaction.account_id as usize % num_partitions;
        let stream_name = format!("transactions_stream_{}", partitioned_index);

        let _: () = redis::cmd("XADD")
            .arg(&stream_name)
            .arg("*")
            .arg("data")
            .arg(tx_data)
            .query_async(&mut con)
            .await?;
        println!("Added transaction to stream: {}", stream_name);

        sleep(Duration::from_millis(10)).await
    }

    Ok(())
}

pub async fn process_using_redis_single_consumer(
    num_partitions: usize,
) -> Result<(), Box<dyn Error>> {
    let mut partitioned_account_groups: Vec<Arc<Mutex<HashMap<u16, Account>>>> =
        Vec::with_capacity(num_partitions);

    for _ in 0..num_partitions {
        partitioned_account_groups.push(Arc::new(Mutex::new(HashMap::new())));
    }

    // For each partitioned, set up a mechanism to avoid race conditions
    for partitioned_index in 0..num_partitions {
        let partitioned_accounts = Arc::clone(&partitioned_account_groups[partitioned_index]);
        let stream_name = format!("transactions_stream_{}", partitioned_index);

        tokio::spawn(async move {
            let client =
                redis::Client::open("redis://127.0.0.1/").expect("Failed to connect to Redis ");

            let mut con = client
                .clone()
                .get_connection()
                .expect("Failed to open connection to client");

            let mut last_id = "0".to_string();

            loop {
                let res: RedisResult<StreamReadReply> = con.xread(&[&stream_name], &[&last_id]);

                match res {
                    Ok(reply) => {
                        for stream_key in reply.keys {
                            for message in stream_key.ids {
                                let tx_data: String = message.get("data").unwrap();
                                let transaction: Transaction =
                                    serde_json::from_str(&tx_data).unwrap();

                                process_transaction_from_arc(transaction, &partitioned_accounts)
                                    .await;

                                // acknowledge that message is processed
                                let _: () = con
                                    .xack(
                                        stream_name.clone(),
                                        "transaction_group",
                                        &[message.id.clone()],
                                    )
                                    .unwrap();

                                last_id = message.id.clone();
                            }
                        }
                    }

                    Err(e) => return e,
                }
            }
        });
    }

    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    // Receive transactions and send to the partitioned Redis stream
    for _ in 0..20 {
        let transaction = receive_transaction().await;

        println!("CREATED: {}", transaction.account_id);
        let tx_data = serde_json::to_string(&transaction)?;

        let partitioned_index = transaction.account_id as usize % num_partitions;
        let stream_name = format!("transactions_stream_{}", partitioned_index);

        let _: () = con.xadd(&stream_name, "*", &[("data", tx_data)])?;

        sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

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
            .send(receive_transaction().await)
            .await
            .unwrap();

        sleep(Duration::from_millis(500)).await;
    }
}
