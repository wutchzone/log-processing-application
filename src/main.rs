#![deny(
    clippy::expect_used,
    clippy::future_not_send,
    clippy::indexing_slicing,
    clippy::panic_in_result_fn,
    clippy::pedantic,
    clippy::string_slice,
    clippy::todo,
    clippy::unreachable,
    clippy::unwrap_used,
    unsafe_code
)]
#![allow(
    clippy::manual_non_exhaustive,
    clippy::missing_errors_doc,
    clippy::module_inception,
    clippy::module_name_repetitions,
    clippy::needless_return,
    clippy::single_match_else,
    clippy::inconsistent_struct_constructor,
    clippy::cast_possible_wrap
)]

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use prost::Message as ProstMessage;
use rdkafka::{
    client::ClientContext,
    config::{ClientConfig, RDKafkaLogLevel},
    consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext, Rebalance},
    error::KafkaResult,
    message::Message,
    topic_partition_list::TopicPartitionList,
};
use tracing_subscriber::{prelude::*, util::SubscriberInitExt, EnvFilter};

use crate::util::{AggregatedKey, CommunicationData};

mod config;
mod flowprotob;
mod influx;
mod util;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka. This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        tracing::info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        tracing::info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        tracing::info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

fn initialize_logging() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout_log = tracing_subscriber::fmt::layer().compact();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_log)
        .init();
}

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    initialize_logging();
    let config = config::Config::parse_or_exit();
    tracing::info!(?config, "Application initialized.");

    let context = CustomContext;
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", &config.group_id)
        .set("bootstrap.servers", &config.brokers)
        // .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        // .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)?;

    consumer.subscribe(
        config
            .topics
            .iter()
            .map(String::as_str)
            .collect::<Vec<&str>>()
            .as_slice(),
    )?;

    let processing_time = Arc::new(AtomicI64::new(0));
    let size_of_cache = Arc::new(AtomicUsize::new(0));
    let total_transferred = Arc::new(AtomicU64::new(0));

    {
        let processing_time = processing_time.clone();
        let size_of_cache = size_of_cache.clone();
        let total_transferred = total_transferred.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let time =
                    chrono::DateTime::from_timestamp(processing_time.load(Ordering::Relaxed), 0);
                let size_of_cache = size_of_cache.load(Ordering::Relaxed);

                tracing::info!(
                    "Latest processed: {time:?}, size of cache: {size_of_cache}, bytes transferred \
                     since last print: {}b",
                    size_format::SizeFormatterBinary::new(
                        total_transferred.load(Ordering::Acquire) * 8
                    )
                );

                total_transferred.store(0, Ordering::Release);
            }
        });
    }

    let client = influxdb2::Client::new(
        config.influxdb_endpoint,
        config.influxdb_org,
        config.influxdb_token,
    );

    let mut edge_cache: HashMap<AggregatedKey, CommunicationData> = HashMap::new();
    loop {
        if size_of_cache.load(Ordering::Relaxed) >= config.batch_size {
            if let Err(error) =
                influx::insert_data_into_influx(&client, &config.influxdb_bucket, &edge_cache).await
            {
                tracing::error!(
                    error = error.to_string(),
                    "Unable to submit data into influx. Sleeping and retrying."
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            tracing::info!(
                cache.bytes = size_of_cache.load(Ordering::Relaxed),
                cache.elements = edge_cache.len(),
                "Inserted new batch into the influx."
            );

            size_of_cache.store(0, Ordering::Relaxed);
            edge_cache.clear();
        }

        match consumer.recv().await {
            Err(error) => tracing::error!("Kafka error: {}", error),
            Ok(message) => {
                if let Some(payload) = message.payload() {
                    let message = flowprotob::FlowMessage::decode(payload)?;
                    total_transferred.fetch_add(message.bytes, Ordering::Relaxed);
                    let Some(src_location) =
                        util::parse_location(message.etype, &message.src_addr, &config.cidr_list)?
                    else {
                        // tracing::warn!("Invalid src location.");
                        continue;
                    };
                    let Some(dst_location) =
                        util::parse_location(message.etype, &message.dst_addr, &config.cidr_list)?
                    else {
                        // tracing::warn!("Invalid dst location.");
                        continue;
                    };

                    let seconds_alignment = 60 * 5; // 5 minutes

                    match edge_cache.entry(AggregatedKey {
                        time: message.time_flow_start.div_euclid(seconds_alignment)
                            * seconds_alignment,
                        source: src_location,
                        target: dst_location,
                        src_vlan: message.src_vlan,
                        dst_vlan: message.dst_vlan,
                        proto: message.proto,
                    }) {
                        Entry::Occupied(mut entry) => {
                            let entry = entry.get_mut();
                            entry.bytes += message.bytes;
                            entry.packets += message.packets;
                        },
                        Entry::Vacant(entry) => {
                            entry.insert(CommunicationData {
                                packets: message.packets,
                                bytes: message.bytes,
                            });
                        },
                    }

                    processing_time.store(i64::try_from(message.time_received)?, Ordering::Relaxed);
                    size_of_cache.fetch_add(
                        std::mem::size_of::<u32>() + payload.len(),
                        Ordering::Relaxed,
                    );
                } else {
                    panic!("Unable to decode.");
                }
            },
        };
    }
}
