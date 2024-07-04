use std::{
    collections::HashMap,
    sync::atomic::{AtomicI64, Ordering},
};

use futures::prelude::*;
use influxdb2::{
    models::{data_point::DataPointError, DataPoint},
    Client,
};

use crate::util::{AggregatedKey, CommunicationData};

static BATCH_NUMBER: AtomicI64 = AtomicI64::new(0);

pub async fn insert_data_into_influx(
    client: &Client,
    bucket_name: &str,
    edge_cache: &HashMap<AggregatedKey, CommunicationData>,
) -> anyhow::Result<()> {
    let batch_number = BATCH_NUMBER.fetch_add(1, Ordering::SeqCst);
    client
        .write(
            bucket_name,
            stream::iter(
                edge_cache
                    .iter()
                    .map(|(key, value)| {
                        DataPoint::builder("sflow")
                            .tag("source", format!("{:?}", key.source))
                            .tag("target", format!("{:?}", key.target))
                            .tag("src_vlan", key.src_vlan.to_string())
                            .tag("dst_vlan", key.dst_vlan.to_string())
                            .tag("proto", key.proto.to_string())
                            // Primary key consists of tags + timestamp. We cannot guarantee that
                            // the same timestamp and tags will not repeat. Therefore must add
                            // something unique to each insert. Otherwise, we could erase already
                            // existing data.
                            .tag("batch_number", batch_number.to_string())
                            .field("packets", value.packets as i64)
                            .field("bytes", value.bytes as i64)
                            // Default time is in seconds but we need it in nanoseconds.
                            .timestamp(key.time as i64 * 1_000_000_000)
                            .build()
                    })
                    .collect::<Result<Vec<DataPoint>, DataPointError>>()?,
            ),
        )
        .await?;

    Ok(())
}
