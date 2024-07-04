use std::env;

use cidr_utils::cidr::IpCidr;
use clap::Parser;

#[derive(Clone, Debug)]
pub struct Config {
    pub group_id: String,
    pub topics: Vec<String>,
    pub brokers: String,
    pub batch_size: usize,
    pub cidr_list: Vec<IpCidr>,

    pub influxdb_token: String,
    pub influxdb_endpoint: String,
    pub influxdb_bucket: String,
    pub influxdb_org: String,
}

impl Config {
    /// Parse from `std::env::args_os()` and/or `std::env::vars()`,
    ///
    /// # Panics
    ///
    /// Panics if a valid configuration cannot be created from environment variables and CLI
    /// arguments.
    #[must_use]
    pub fn parse_or_exit() -> Self {
        #[allow(clippy::expect_used)]
        ConfigArgs::parse()
            .try_into()
            .expect("Failed to convert ConfigArgs to Config.")
    }
}

// ⚠️ If you add any ENVs here, consider updating `config.dist.toml` and `postinst`. ⚠️
#[derive(Parser, Debug)]
#[clap(author, about, long_version = long_version(), version = version())]
pub struct ConfigArgs {
    /// Consumer group id.
    #[clap(long, value_parser, env = "KAFKA_DUMP_GROUP_ID")]
    group_id: String,

    /// Topics from which read.
    #[clap(
        long,
        value_parser,
        value_delimiter = ',',
        env = "KAFKA_DUMP_TOPICS",
        required = true
    )]
    topics: Vec<String>,

    /// Kafka brokers in Kafka format.
    #[clap(long, value_parser, env = "KAFKA_DUMP_BROKERS")]
    brokers: String,

    #[clap(long, value_parser, env = "KAFKA_DUMP_INFLUXDB_TOKEN")]
    influxdb_token: String,

    #[clap(long, value_parser, env = "KAFKA_DUMP_INFLUXDB_ENDPOINT")]
    influxdb_endpoint: String,

    #[clap(long, value_parser, env = "KAFKA_DUMP_INFLUXDB_BUCKET")]
    influxdb_bucket: String,

    #[clap(long, value_parser, env = "KAFKA_DUMP_INFLUXDB_ORG")]
    influxdb_org: String,

    #[clap(
        long,
        value_parser,
        value_delimiter = ',',
        env = "KAFKA_DUMP_CIDR_LIST",
        required = true
    )]
    cidr_list: Vec<IpCidr>,

    #[clap(long, value_parser, env = "KAFKA_DUMP_BATCH_SIZE")]
    batch_size: usize,
}

impl TryFrom<ConfigArgs> for Config {
    type Error = anyhow::Error;

    fn try_from(value: ConfigArgs) -> Result<Self, Self::Error> {
        let ConfigArgs {
            group_id,
            topics,
            brokers,
            influxdb_token,
            influxdb_endpoint,
            influxdb_bucket,
            influxdb_org,
            cidr_list,
            batch_size,
        } = value;

        Ok(Self {
            group_id,
            topics,
            brokers,
            influxdb_token,
            influxdb_endpoint,
            influxdb_bucket,
            batch_size,
            cidr_list,
            influxdb_org,
        })
    }
}

const fn version() -> &'static str {
    concat!(env!("CARGO_PKG_VERSION"), " git:", env!("VERGEN_GIT_SHA"))
}

const fn long_version() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        "\n",
        "\nCommit SHA:\t",
        env!("VERGEN_GIT_SHA"),
        "\nCommit Date:\t",
        env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
        "\nrustc Version:\t",
        env!("VERGEN_RUSTC_SEMVER"),
        "\nrustc SHA:\t",
        env!("VERGEN_RUSTC_COMMIT_HASH"),
        "\ncargo Target:\t",
        env!("VERGEN_CARGO_TARGET_TRIPLE"),
        "\ncargo Profile:\t",
        env!("VERGEN_CARGO_PROFILE"),
    )
}
