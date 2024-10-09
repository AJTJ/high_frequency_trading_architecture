use rand::Rng;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Transaction {
    pub account_id: u16,
    pub transaction_type: TransactionType,
    pub amount: f64,
}

#[derive(Debug)]
pub struct Account {
    pub balance: f64,
}

impl Account {
    pub fn new() -> Self {
        Account { balance: 0.0 }
    }

    pub fn deposit(&mut self, amount: f64) {
        self.balance += amount;
    }

    pub fn withdrawal(&mut self, amount: f64) {
        self.balance -= amount;
    }
}

// A mock endpoint to simulate receiving transactions intermittently
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

// A endpoint to simulate processing transactions by a worker thread
pub async fn process_transaction_from_arc(
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
pub async fn receive_transaction() -> Transaction {
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
