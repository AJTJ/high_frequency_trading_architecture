use crate::redis_consumer_groups::config::NUM_PARTITIONS;
use crate::redis_utils::add_message_to_stream;
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;

use super::queue_management::RedisClientArc;
use super::tx_models::Transaction;

pub async fn transaction_ingestion(
    txs: Vec<Transaction>,
    consumer_groups: RedisClientArc,
) -> Result<(), Box<dyn Error>> {
    let mut con = consumer_groups.get_multiplexed_async_connection().await?;

    // Receive transactions and send to the partitioned Redis consumer group
    for transaction in txs {
        let tx_data = serde_json::to_string(&transaction)?;

        let partitioned_index = transaction.account_id as usize % NUM_PARTITIONS;
        let stream_name = format!("transactions_stream_{}", partitioned_index);

        let _ = add_message_to_stream(&stream_name, &tx_data, &mut con);

        sleep(Duration::from_millis(10)).await
    }

    Ok(())
}
