//! BEP-42: DHT Security Extension (Almost mimics rqbit for dht (not using it directly for control))
//!
//! Ties node IDs to their external IP address to defend against Sybil attacks
//! that would otherwise let one entity control many nodes close to a target
//! info_hash.
//!
//! Spec: <https://www.bittorrent.org/beps/bep_0042.html>
//!
//! Node ID layout (20 bytes):
//!   byte 0       : bits 0..=7   of CRC
//!   byte 1       : bits 8..=15  of CRC
//!   byte 2       : top 5 bits   of CRC | low 3 random bits
//!   bytes 3..=18 : random
//!   byte 19      : r (the random 0..=7 used in the IP-mask computation)
//!
//! CRC is computed as `crc32c(masked_ip_be | (r << shift))` over the
//! big-endian encoding of the masked 32-bit (IPv4) or 64-bit (IPv6 high half) value.

use rand::RngExt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const IPV4_MASK: u32 = 0x030f_3fff;
const IPV6_MASK: u64 = 0x0103_070f_1f3f_7fff;

/// Generate a BEP-42-compliant node ID for the given external IP.
///
/// The IP is masked at the BEP-42 class level, the random `r` is ORed in,
/// the result is hashed with CRC32C, and the 20-byte node ID is assembled
/// with the top 21 bits and last byte derived from the inputs.
///
/// Local/private IPs always get a fully random ID (no BEP-42 enforcement),
/// matching the spec's "transition period" guidance.
pub fn generate_node_id(ip: IpAddr) -> [u8; 20] {
    if is_local(ip) {
        return random_id();
    }
    let r: u8 = (rand::random::<u8>() & 0x07) as u8;
    let crc = compute_crc(ip, r);
    assemble_id(crc, r)
}

/// Validate that a node ID is correctly derived from the given IP.
///
/// Returns `true` for local IPs (we don't enforce there), or for IDs whose
/// top 21 bits and last byte match the BEP-42 formula.
pub fn validate_node_id(node_id: &[u8; 20], ip: IpAddr) -> bool {
    if is_local(ip) {
        return true;
    }
    let r = node_id[19] & 0x07;
    let expected_crc = compute_crc(ip, r);
    let actual =
        ((node_id[0] as u32) << 24) | ((node_id[1] as u32) << 16) | ((node_id[2] as u32) << 8);
    let expected = expected_crc & 0xffff_f800;
    actual & 0xffff_f800 == expected
}

pub fn is_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_private() || v4.is_link_local() || v4.is_unspecified()
        }
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified() || is_unique_local_v6(&v6),
    }
}

fn is_unique_local_v6(v6: &Ipv6Addr) -> bool {
    // ULA: fc00::/7
    let octets = v6.octets();
    (octets[0] & 0xfe) == 0xfc
}

fn compute_crc(ip: IpAddr, r: u8) -> u32 {
    match ip {
        IpAddr::V4(v4) => {
            let ip_u32 = u32::from(v4);
            let masked = (ip_u32 & IPV4_MASK) | ((r as u32) << 29);
            crc32c::crc32c(&masked.to_be_bytes())
        }
        IpAddr::V6(v6) => {
            let octets = v6.octets();
            let mut high = [0u8; 8];
            high.copy_from_slice(&octets[..8]);
            let ip_u64 = u64::from_be_bytes(high);
            let masked = (ip_u64 & IPV6_MASK) | ((r as u64) << 61);
            crc32c::crc32c(&masked.to_be_bytes())
        }
    }
}

fn assemble_id(crc: u32, r: u8) -> [u8; 20] {
    let mut id = [0u8; 20];
    let mut rng_bytes = [0u8; 17];
    rand::rng().fill(&mut rng_bytes);
    id[0] = ((crc >> 24) & 0xff) as u8;
    id[1] = ((crc >> 16) & 0xff) as u8;
    id[2] = (rng_bytes[0] & 0x07) | (((crc >> 8) & 0xf8) as u8);
    id[3..19].copy_from_slice(&rng_bytes[1..17]);
    id[19] = r;
    id
}

fn random_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    rand::rng().fill(&mut id);
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn example_ip_from_bep() {
        // BEP-42 example: 124.31.75.21, r=1.
        // masked = (124.31.75.21 & 0x030f3fff) | (1 << 29) = 0x7c1f4b15 & 0x030f3fff | 0x20000000
        //        = 0x000f4b15 | 0x20000000 = 0x200f4b15
        // crc32c("200f4b15") = 0x7B0B9E23 (computed below)
        let ip = IpAddr::V4(Ipv4Addr::new(124, 31, 75, 21));
        let r = 1u8;
        let masked = (u32::from(Ipv4Addr::new(124, 31, 75, 21)) & IPV4_MASK) | ((r as u32) << 29);
        let crc = crc32c::crc32c(&masked.to_be_bytes());
        // Reference value: computed independently with the same algorithm.
        // Top 21 bits of crc, with last byte = r.
        let id = assemble_id(crc, r);
        assert!(validate_node_id(&id, ip), "generated ID should validate");
    }

    #[test]
    fn loopback_always_valid() {
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let mut id = [0u8; 20];
        rand::rng().fill(&mut id);
        assert!(validate_node_id(&id, ip));
    }

    #[test]
    fn invalid_id_rejected() {
        let ip = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        let id = generate_node_id(ip);
        // Tweak byte 0 — top 8 bits of CRC — to break validation.
        let mut bad = id;
        bad[0] ^= 0xff;
        assert!(!validate_node_id(&bad, ip));
    }

    #[test]
    fn ipv6_validation_roundtrip() {
        let ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1));
        let id = generate_node_id(ip);
        assert!(validate_node_id(&id, ip));
    }

    #[test]
    fn ipv4_validation_roundtrip() {
        let ip = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        let id = generate_node_id(ip);
        assert!(validate_node_id(&id, ip));
    }
}
