use exchange_v2::{
    process_using_mpsc::process_transactions_using_mpsc,
    process_using_redis_consumer_groups::process_using_redis_consumer_groups,
    process_using_single_consumer::process_using_redis_single_consumer,
};

#[tokio::main]
async fn main() {
    let num_partitions = 4;

    // FIRST EXPERIMENT. using mpsc
    // The problem with mpsc is that it tightly couples the sender and receive and limits scaling potential
    process_transactions_using_mpsc(num_partitions).await;

    // SECOND EXPERIMENT. Ok, considering it uses redis, but it doesn't use the power of consumer groups
    // It's limiting factor is that you only have one process per partition processing transactions
    if let Err(e) = process_using_redis_single_consumer(num_partitions).await {
        eprintln!("SINGLE CONSUMER with redis error: {}", e);
    }

    // THIRD EXPERIMENT. This is by far the best example, using redis consumer groups
    // It would segue nicely into a Kafka setup
    // NOTE: you would need to flush data between runs:
    // `redis-cli FLUSHALL`
    if let Err(e) = process_using_redis_consumer_groups(num_partitions).await {
        eprintln!("CONSUMER GROUPS with redis error: {}", e);
    }
}
