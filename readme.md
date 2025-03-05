# Description
Experiments into how I would begin to architect a high frequency trading platform.

## The goal
Given a set of incoming transactions, how would I build a single modular service that has high thoroughput while ensuring message ordering and account-level consistency.

## The outcome
TLDR, Go to `main.rs` to begin exploring the code.
I found that using Redis consumer groups in this POC illustrated how the architecture would transition nicely into a Kafka system. Separating servicers into `transaction_ingestion`, `queue_management` and `transaction_processing` sets the stage for a more fine-grained separation of concerns.

## Main takeaway
Using the power of consumer groups, a concept common to redis, kafka and other messaging services, I am able to maintain message ordering while also ensuring account-level consistency.

# Future Scaling plan:

## Input and Queuing
The reception of transactions from a TCP stream or API could be handled by an asynchronous service responsible for enqueuing transactions sequentially (in order received) in a dedicated in-memory queue. This decouples the I/O bound receiving phase from the CPU-bound processing phase, thus improving throughput.
I would also consider sharding by Account ID for each queue. So, rather than having a single queue for all transactions we would have multiple queues, each dedicated to a subset of Account IDs. This removes the single queue as a potential bottleneck.

## Processing
For transaction processing, I would use a pool of workers threads each responsible for processing transactions for a subset of accounts. This ensures that all transactions for a given account are processed synchronously within that worker, avoiding race conditions and maintaining account-level consistency. We would still achieve concurrent processing across different shards.
I would separate transaction validation and account state management into two distinct services for ease of maintainability and feature addition. 

## Database operations
To reduce the overhead of many database writes, each worker could batch account updates in-memory before committing them to the database. The batch size could be tuned to balance latency and database interactions.

## Fault tolerance and latency
Fault tolerance and latency are crucial. And a crash could cause data loss. I'd ensure that the queue(s) is durable and supports persistence or replication to disk. Implementing retry logic and backpressure maintenance would ensure that no transactions are dropped or processed out of order due to system failures or delays.

# Is there a simpler way? Yea sure:...

...A very simple multi-threaded version would be to wrap a Mutex around each account. The mutex lock would avoid any race conditions and maintain account-level consistency. Mutexes are blocking constructs though, and will force threads to wait for the mutex to be released. This leads to contention and performance bottlenecks. Deadlocks might also occur if you are not careful.

...Another simpler solution would be to shard accounts into separate buckets in a HashMap. As above with Mutexes, I could lock each bucket if it’s being worked on. While this is superior to the single-threaded environment, it introduces coarse-grained locking, and load balancing becomes an issue if some buckets have more transactions than others.

...It could be possible to explore a lock-free or wait-free system. But this introduces a lot of complexity in maintaining account-level consistency.

...An actor model could be a simple approach, by having one actor per account. Thet trade off is that maintaining that sort of system with messaging between actors can be complex.