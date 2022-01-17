#![allow(dead_code)]

use std::collections::HashMap;
use std::env::{self, VarError};
use std::io::Write;


use chrono::prelude::*;
use env_logger::fmt::Formatter;

use std::time::Duration;


use env_logger::Builder;

use log::{LevelFilter, Record};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaResult;
use rdkafka::message::ToBytes;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::statistics::Statistics;
use rdkafka::TopicPartitionList;
use regex::Regex;

pub fn rand_test_topic() -> String {
    let mut trng = rand::thread_rng();
    let id = std::iter::repeat(()).map(|()| trng.sample(Alphanumeric))
        .map(char::from)
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_group() -> String {
    let mut trng = rand::thread_rng();
    let id = std::iter::repeat(()).map(|()| trng.sample(Alphanumeric))
        .map(char::from)
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn rand_test_transactional_id() -> String {
    let mut trng = rand::thread_rng();
    let id = std::iter::repeat(()).map(|()| trng.sample(Alphanumeric))
        .map(char::from)
        .take(10)
        .collect::<String>();
    format!("__test_{}", id)
}

pub fn get_bootstrap_server() -> String {
    env::var("KAFKA_HOST").unwrap_or_else(|_| "localhost:9092".to_owned())
}

pub fn get_broker_version() -> KafkaVersion {
    // librdkafka doesn't expose this directly, sadly.
    match env::var("KAFKA_VERSION") {
        Ok(v) => {
            let regex = Regex::new(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:\.(\d+))?$").unwrap();
            match regex.captures(&v) {
                Some(captures) => {
                    let extract = |i| {
                        captures
                            .get(i)
                            .map(|m| m.as_str().parse().unwrap())
                            .unwrap_or(0)
                    };
                    KafkaVersion(extract(1), extract(2), extract(3), extract(4))
                }
                None => panic!("KAFKA_VERSION env var was not in expected [n[.n[.n[.n]]]] format"),
            }
        }
        Err(VarError::NotUnicode(_)) => {
            panic!("KAFKA_VERSION env var contained non-unicode characters")
        }
        // If the environment variable is unset, assume we're running the latest version.
        Err(VarError::NotPresent) => {
            KafkaVersion(std::u32::MAX, std::u32::MAX, std::u32::MAX, std::u32::MAX)
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct KafkaVersion(pub u32, pub u32, pub u32, pub u32);

pub struct ProducerTestContext {
    _some_data: i64, // Add some data so that valgrind can check proper allocation
}

impl ClientContext for ProducerTestContext {
    fn stats(&self, _: Statistics) {} // Don't print stats
}

pub async fn create_topic(name: &str, partitions: i32) {
    let client: AdminClient<_> = consumer_config("create_topic", None)
        .into_iter().collect::<ClientConfig>().create().unwrap();
    client
        .create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
}

pub fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |formatter: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("(t: {}) ", std::thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        writeln!(
            formatter,
            "{} {}{} - {} - {}",
            time_str,
            thread_name,
            record.level(),
            record.target(),
            record.args()
        )
    };

    let mut builder = Builder::new();
    builder
        .format(output_format)
        .filter(None, LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}

/// Produce the specified count of messages to the topic and partition specified. A map
/// of (partition, offset) -> message id will be returned. It panics if any error is encountered
/// while populating the topic.
pub async fn populate_topic<P, K, J, Q>(
    topic_name: &str,
    count: i32,
    value_fn: &P,
    key_fn: &K,
    partition: Option<i32>,
    timestamp: Option<i64>,
) -> HashMap<(i32, i64), i32>
    where
        P: Fn(i32) -> J,
        K: Fn(i32) -> Q,
        J: ToBytes,
        Q: ToBytes,
{
    let prod_context = ProducerTestContext { _some_data: 1234 };

    // Produce some messages
    let producer = &ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_server().as_str())
        .set("statistics.interval.ms", "500")
        .set("api.version.request", "true")
        .set("debug", "all")
        .set("message.timeout.ms", "30000")
        .create_with_context::<ProducerTestContext, FutureProducer<_>>(prod_context)
        .expect("Producer creation error");

    let futures = (0..count)
        .map(|id| {
            let future = async move {
                producer
                    .send(
                        FutureRecord {
                            topic: topic_name,
                            payload: Some(&value_fn(id)),
                            key: Some(&key_fn(id)),
                            partition,
                            timestamp,
                            headers: None,
                        },
                        Duration::from_secs(1),
                    )
                    .await
            };
            (id, future)
        })
        .collect::<Vec<_>>();

    let mut message_map = HashMap::new();
    for (id, future) in futures {
        match future.await {
            Ok((partition, offset)) => message_map.insert((partition, offset), id),
            Err((kafka_error, _message)) => panic!("Delivery failed: {}", kafka_error),
        };
    }

    message_map
}

pub fn value_fn(id: i32) -> String {
    format!("Message {}", id)
}

pub fn key_fn(id: i32) -> String {
    format!("Key {}", id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_populate_topic() {
        let topic_name = rand_test_topic();
        let message_map = populate_topic(&topic_name, 100, &value_fn, &key_fn, Some(0), None).await;

        let total_messages = message_map
            .iter()
            .filter(|&(&(partition, _), _)| partition == 0)
            .count();
        assert_eq!(total_messages, 100);

        let mut ids = message_map.iter().map(|(_, id)| *id).collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (0..100).collect::<Vec<_>>());
    }
}

pub struct ConsumerTestContext {
    pub _n: i64, // Add data for memory access validation
}

impl ClientContext for ConsumerTestContext {
    // Access stats
    fn stats(&self, stats: Statistics) {
        let stats_str = format!("{:?}", stats);
        log::info!("Stats received: {} bytes", stats_str.len());
    }
}

impl ConsumerContext for ConsumerTestContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        log::info!("Committing offsets: {:?}", result);
    }
}

pub fn consumer_config<'a>(
    group_id: &'a str,
    config_overrides: Option<HashMap<&'a str, &'a str>>,
) -> HashMap<String, String> {
    let mut config: HashMap<String, String> = HashMap::new();

    config.insert("group.id".into(), group_id.into());
    config.insert("client.id".into(), "datafusion-streams".into());
    config.insert("bootstrap.servers".into(), get_bootstrap_server());
    config.insert("enable.partition.eof".into(), "true".into());
    config.insert("session.timeout.ms".into(), "6000".into());
    config.insert("enable.auto.commit".into(), "true".into());
    config.insert("statistics.interval.ms".into(), "500".into());
    config.insert("api.version.request".into(), "true".into());
    config.insert("debug".into(), "all".into());
    config.insert("auto.offset.reset".into(), "earliest".into());

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.insert(key.into(), value.into());
        }
    }

    config
}
