use redis::streams::StreamReadReply;
use redis::{Commands, RedisResult};

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::account_and_transaction::{
    process_transaction_from_arc, receive_transaction, Account, Transaction,
};
use crate::redis_utils::get_redis_client;

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
            let client = get_redis_client().expect("Failed to open connection to Redis");

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

        // sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}
