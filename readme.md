# What is this?
It's some experiments into how I would begin to think about a high frequency trading platform.

## Main takeaway
Using the power of consumer groups, a concept common to redis, kafka and other messaging services, you are able to maintain message ordering while also ensuring account-level consistency.

## What's happening?
Given a set of transactions, how would you create an intermediary rust solution (one service) before going fully distributed?
Go check out `main.rs` for some of my experiments. And jump into the `lib.rs` for the code.

## Did I achieve all of the following ideal goals?
No, but I built out the scaffolding for the architecture spefically in terms of achieving a high level of concurrency while maintaining account-level consistency.

# Ideal goals

## Input and Queuing
First off, the reception of transactions from a TCP stream or API could be handled by an asynchronous service responsible for enqueuing transactions sequentially (in order received) in a dedicated in-memory queue. This decouples the I/O bound receiving phase from the CPU-bound processing phase improving throughput.
I would also consider sharding by Account ID for each queue. So, rather than having a single queue for all transactions we would have multiple queues, each dedicated to a subset of Account IDs. This removes the single queue as a potential bottleneck.

## Processing
For transaction processing, I would use a pool of workers threads each responsible for processing transactions for a subset of accounts. This ensures that all transactions for a given account are processed synchronously within that worker, avoiding race conditions and maintaining account-level consistency. We would still achieve concurrent processing across different shards.
I would separate transaction validation and account state management into two distinct services for ease of maintainability and feature addition. 

## Database operations
To reduce the overhead of many database writes, each worker could batch account updates in-memory before committing them to the database. The batch size could be tuned to balance latency and database interactions.

## Fault tolerance and latency
Fault tolerance and latency are crucial. And a crash could cause data loss. I'd ensure that the queue(s) is durable and supports persistence or replication to disk. Implementing retry logic and backpressure maintenance would ensure that no transactions are dropped or processed out of order due to system failures or delays.

## Scalability
The nice thing about this setup is that segues nicely into future scaling. The worker pool (and queues) could be expanded across multiple servers or nodes with minimal changes to the core architecture. It would also be a fairly seamless transition to using Kafka. 

## Is there a simpler way? Yea sure.
There are certainly simpler versions of this that I could list.

A very simple multi-threaded version would be to wrap a Mutex around each account. The mutex lock would avoid any race conditions and maintain account-level consistency. Mutexes are blocking constructs though, and will force threads to wait for the mutex to be released. This leads to contention and performance bottlenecks. Deadlocks might also occur if you are not careful.

Another simpler solution would be to shard accounts into separate buckets in a HashMap. As above with Mutexes, I could lock each bucket if it’s being worked on. While this is superior to the single-threaded environment, it introduces coarse-grained locking, and load balancing becomes an issue if some buckets have more transactions than others.

It could be possible to explore a lock-free or wait-free system. But this introduces a lot of complexity in maintaining account-level consistency.

An actor model could be a simple approach, by having one actor per account. Maintaining that sort of system with messaging can get complex.