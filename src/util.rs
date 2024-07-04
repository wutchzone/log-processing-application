use std::net::IpAddr;

use anyhow::anyhow;
use cidr_utils::cidr::IpCidr;
use serde::{Serialize, Serializer};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
pub enum Location {
    Inside(IpAddr),
    Outside,
}

impl Serialize for Location {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&match self {
            Location::Inside(ip) => ip.to_string(),
            Location::Outside => "outside".to_owned(),
        })
    }
}

#[derive(Serialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct AggregatedKey {
    pub time: u64,
    pub source: Location,
    pub target: Location,
    pub src_vlan: u32,
    pub dst_vlan: u32,
    pub proto: u32,
}

#[derive(Serialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct CommunicationData {
    pub packets: u64,
    pub bytes: u64,
}

fn parse_ip(etype: u32, addr: &Vec<u8>) -> anyhow::Result<Option<IpAddr>> {
    match etype {
        0x0800 => {
            let ipv4: [u8; 4] = addr.as_slice().try_into().map_err(|error| {
                tracing::error!(addr = ?error, "Failed to create IPv4.");
                anyhow!("Received invalid addr for given `etype`.")
            })?;
            Ok(Some(IpAddr::from(ipv4)))
        },
        0x86DD if addr.len() == 16 => {
            let ipv6: [u8; 16] = addr.as_slice().try_into().map_err(|error| {
                tracing::error!(addr = ?error, "Failed to create IPv6.");
                anyhow!("Received invalid addr for given `etype`.")
            })?;
            Ok(Some(IpAddr::from(ipv6)))
        },
        // ARP
        0x0806 => Ok(None),
        etype => {
            tracing::warn!("Unknown etype: {etype:X}, addr: {addr:?}.");

            Ok(None)
        },
    }
}

pub fn parse_location(
    etype: u32,
    addr: &Vec<u8>,
    cidr_list: &Vec<IpCidr>,
) -> anyhow::Result<Option<Location>> {
    Ok(if let Some(ip) = parse_ip(etype, addr)? {
        for cidr in cidr_list {
            if cidr.contains(ip) {
                return Ok(Some(Location::Inside(ip)));
            }
        }
        Some(Location::Outside)
    } else {
        None
    })
}
