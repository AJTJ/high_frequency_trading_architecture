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
