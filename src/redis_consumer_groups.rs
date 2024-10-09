use crate::account_and_transaction::{
    process_transaction_from_mutex_guard, receive_transaction, Account, Transaction,
};
use crate::redis_utils::{
    acknowledge_message, add_message_to_stream, create_redis_consumer_group, get_redis_client,
    read_from_redis_stream,
};
use dashmap::DashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

pub async fn process_using_redis_consumer_groups(
    num_partitions: usize,
) -> Result<(), Box<dyn Error>> {
    let client = get_redis_client()?;

    let mut partitioned_account_groups: Vec<Arc<DashMap<u16, Arc<Mutex<Account>>>>> =
        Vec::with_capacity(num_partitions);

    // push a group of partition of accounts to the groups
    for _ in 0..num_partitions {
        partitioned_account_groups.push(Arc::new(DashMap::new()));
    }

    let client_arc = Arc::new(client);

    for partition_index in 0..num_partitions {
        let stream_name = format!("transactions_stream_{}", partition_index);
        let consumer_group_name = format!("consumer_group_{}", partition_index);
        let consumer_name = format!("consumer_{}", partition_index);

        create_redis_consumer_group(&stream_name, &consumer_group_name, &client_arc).await?;

        // Create four workers per partition
        for worker_id in 0..4 {
            let stream_name = stream_name.clone();
            let consumer_name = format!("{}_worker_{}", consumer_name, worker_id);
            let client_clone = Arc::clone(&client_arc);
            let consumer_group_name = consumer_group_name.clone();

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
                    let res = read_from_redis_stream(
                        &stream_name,
                        &consumer_group_name,
                        &consumer_name,
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
                                    let _ = acknowledge_message(
                                        &stream_name,
                                        &consumer_group_name,
                                        &message.id,
                                        &mut con,
                                    )
                                    .await;
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

        let _ = add_message_to_stream(&stream_name, &tx_data, &mut con);

        println!("Added transaction to stream: {}", stream_name);

        sleep(Duration::from_millis(10)).await
    }

    Ok(())
}
