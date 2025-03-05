use rand::Rng;
use tokio::sync::MutexGuard;

use super::tx_models::{Account, Transaction, TransactionType};

pub async fn process_transaction_from_mutex_guard(
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
