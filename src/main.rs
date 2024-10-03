use exchange_v2::{
    process_transactions_using_mpsc, process_using_redis_consumer_groups,
    process_using_redis_single_consumer,
};

#[tokio::main]
async fn main() {
    let num_partitions = 4;

    process_transactions_using_mpsc(num_partitions).await;

    if let Err(e) = process_using_redis_single_consumer(num_partitions).await {
        eprintln!("Process with redis error: {}", e);
    }

    if let Err(e) = process_using_redis_consumer_groups(num_partitions).await {
        eprintln!("Process with redis error: {}", e);
    }
}
