use exchange_v2::{
    process_using_mpsc::process_transactions_using_mpsc,
    process_using_single_consumer::process_using_redis_single_consumer,
    redis_consumer_groups::{
        queue_management::{start_consumer_groups, RedisClientArc},
        transaction_ingestion::transaction_ingestion,
        tx_models::{Transaction, TransactionType},
    },
    redis_utils::get_redis_client,
};
use rand::Rng;
use std::{error::Error, sync::Arc};

// A mock endpoint to create randomized transactions
pub fn generate_transaction() -> Transaction {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    //
    // Redis with consuper groups.
    //
    // NOTE: you would need to flush data between runs:
    // `redis-cli FLUSHALL`

    let client = get_redis_client()?;
    let client_arc: RedisClientArc = Arc::new(client);
    start_consumer_groups(client_arc.clone()).await?;

    let transactions: Vec<Transaction> = (0..20).map(|_| generate_transaction()).collect();

    transaction_ingestion(transactions, client_arc.clone()).await?;

    //
    // The following were earlier experiments:
    //

    // FIRST EXPERIMENT. using mpsc
    // The problem with mpsc is that it tightly couples the sender and receive and limits scaling potential
    process_transactions_using_mpsc().await;

    // SECOND EXPERIMENT. This went well, considering it uses redis, but it doesn't use the power of consumer groups
    // It's limiting factor is that you only have one process per partition processing transactions.
    process_using_redis_single_consumer().await?;

    Ok(())
}
