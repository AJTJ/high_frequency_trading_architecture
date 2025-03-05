use crate::redis_consumer_groups::transaction_processing::process_transaction_from_mutex_guard;
use crate::redis_consumer_groups::tx_models::{Account, Transaction};
use crate::redis_utils::{
    acknowledge_message, create_redis_consumer_group, read_from_redis_stream,
};
use dashmap::DashMap;
use redis::Client;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::config::{NUM_PARTITIONS, WORKER_PER_PARTITION};

pub type RedisClientArc = Arc<Client>;

#[derive(Clone)]
struct StreamName(String);

#[derive(Clone)]
struct ConsumerGroupName(String);

#[derive(Clone)]
struct ConsumerName(String);

pub async fn start_consumer_groups(client_arc: RedisClientArc) -> Result<(), Box<dyn Error>> {
    let mut partitioned_account_groups: Vec<Arc<DashMap<u16, Arc<Mutex<Account>>>>> =
        Vec::with_capacity(NUM_PARTITIONS);

    // push a group of partition of accounts to the groups
    for _ in 0..NUM_PARTITIONS {
        partitioned_account_groups.push(Arc::new(DashMap::new()));
    }

    for partition_index in 0..NUM_PARTITIONS {
        let stream_name = StreamName(format!("transactions_stream_{}", partition_index));
        let consumer_group_name = ConsumerGroupName(format!("consumer_group_{}", partition_index));

        let consumer_name = ConsumerName(format!("consumer_{}", partition_index));

        create_redis_consumer_group(&stream_name.0, &consumer_group_name.0, &client_arc).await?;

        // Create workers per partition
        for worker_id in 0..WORKER_PER_PARTITION {
            tokio::spawn(process_transaction_worker(
                stream_name.clone(),
                consumer_group_name.clone(),
                consumer_name.clone(),
                client_arc.clone(),
                (&partitioned_account_groups[partition_index]).clone(),
                worker_id,
            ));
        }
    }

    Ok(())
}

async fn process_transaction_worker(
    stream_name: StreamName,
    consumer_group_name: ConsumerGroupName,
    consumer_name: ConsumerName,
    client_clone: RedisClientArc,
    accounts: Arc<DashMap<u16, Arc<Mutex<Account>>>>,
    worker_id: usize,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!(
        "Starting worker {} for partition {}",
        worker_id, stream_name.0
    );
    let mut con = client_clone
        .get_multiplexed_async_connection()
        .await
        .expect("Failed to open async connection");

    loop {
        // Read from Redis stream as part of the consumer group
        let res = read_from_redis_stream(
            &stream_name.0,
            &consumer_group_name.0,
            &consumer_name.0,
            &mut con,
        )
        .await;

        match res {
            Ok(reply) => {
                for stream_key in reply.keys {
                    for message in stream_key.ids {
                        let tx_data: String = match message.get::<String>("data") {
                            Some(data) => data.to_string(),
                            None => {
                                return Err(Box::<dyn Error + Send + Sync>::from(
                                    "Failed to get data from message",
                                ));
                            }
                        };

                        let tx: Transaction = match serde_json::from_str(&tx_data) {
                            Ok(tx) => tx,
                            Err(err) => {
                                eprintln!("Failed to deserialize transaction: {}", err);
                                continue;
                            }
                        };

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

                        let account = account_mutex.lock().await;

                        // Process the transaction, doing something with the outcome
                        let _tx_result = process_transaction_from_mutex_guard(tx, account).await;

                        // Acknowledge the message to redis
                        let _ = acknowledge_message(
                            &stream_name.0,
                            &consumer_group_name.0,
                            &message.id,
                            &mut con,
                        )
                        .await;
                    }
                }
            }
            Err(e) => {
                return Err(Box::<dyn Error + Send + Sync>::from(e.to_string()));
            }
        }
    }
}
