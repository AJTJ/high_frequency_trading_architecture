use redis::aio::MultiplexedConnection;
use redis::streams::StreamReadReply;
use redis::{cmd, Client, RedisResult};

pub fn get_redis_client() -> RedisResult<Client> {
    redis::Client::open("redis://127.0.0.1/")
}

pub async fn create_redis_consumer_group(
    stream_name: &str,
    consumer_group_name: &str,
    client: &Client,
) -> RedisResult<()> {
    let mut con = client.get_multiplexed_async_connection().await?;
    redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream_name)
        .arg(consumer_group_name)
        .arg("$")
        .arg("MKSTREAM")
        .query_async(&mut con)
        .await
}

pub async fn read_from_redis_stream(
    stream_name: &str,
    consumer_group_name: &str,
    consumer_name: &str,
    con: &mut MultiplexedConnection,
) -> RedisResult<StreamReadReply> {
    cmd("XREADGROUP")
        .arg("GROUP")
        .arg(consumer_group_name)
        .arg(consumer_name)
        .arg("BLOCK")
        .arg(0) // Wait indefinitely for new messages
        .arg("COUNT")
        .arg(1) // Fetch one message at a time
        .arg("STREAMS")
        .arg(stream_name)
        .arg(">")
        .query_async(con)
        .await
}

pub async fn acknowledge_message(
    stream_name: &str,
    consumer_group_name: &str,
    message_id: &str,
    con: &mut MultiplexedConnection,
) -> RedisResult<()> {
    cmd("XACK")
        .arg(stream_name)
        .arg(consumer_group_name)
        .arg(message_id)
        .query_async(con)
        .await
}

pub async fn add_message_to_stream(
    stream_name: &str,
    tx_data: &str,
    con: &mut MultiplexedConnection,
) -> RedisResult<()> {
    cmd("XADD")
        .arg(stream_name)
        .arg("*")
        .arg("tx_data")
        .arg(tx_data)
        .query_async(con)
        .await
}
