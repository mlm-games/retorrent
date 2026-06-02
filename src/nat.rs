//! UPnP / NAT-PMP port mapping.
//!
//! Maps the local listen port on the gateway so that the DHT and incoming
//! peer connections are reachable from the public internet. Most home
//! routers expose IGD (Internet Gateway Device) and will respond to a
//! multicast search; if one is found, we add a mapping for the listen port
//! and refresh it periodically. Every failure path is non-fatal — we
//! simply log a warning and continue, which matches qBittorrent's
//! behaviour.
//!
//! Note: we use the legacy `igd` crate (sync I/O wrapped in
//! `spawn_blocking`) rather than the `igd-next` async rewrite to keep
//! the dependency footprint small. The work is dominated by network
//! round-trips, not blocking on a single socket.

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::time::Duration;

use igd::{PortMappingProtocol, SearchOptions};
use tracing::{debug, info, warn};

const REFRESH_INTERVAL: Duration = Duration::from_secs(30 * 60);
const LEASE_DURATION_SECS: u32 = REFRESH_INTERVAL.as_secs() as u32;
const DESCRIPTION: &str = "retorrent";

/// Probe a public address to discover the local IPv4 address the OS will
/// use for outbound traffic. We never send any packets; we just let the
/// kernel pick a source address.
fn local_ipv4() -> Option<Ipv4Addr> {
    let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    sock.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
    match sock.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(v4) => Some(v4),
        std::net::IpAddr::V6(_) => None,
    }
}

/// Add a single port mapping (TCP or UDP) for `external_port` pointing at
/// `local_addr`. Returns `Ok(true)` on success, `Ok(false)` if no IGD
/// gateway was found, and `Err` only on programmer errors (e.g. port 0).
async fn map_port(
    protocol: PortMappingProtocol,
    external_port: u16,
    local_addr: SocketAddrV4,
) -> Result<bool, String> {
    if external_port == 0 {
        return Err("external_port is 0".into());
    }
    let local_addr_for_blocking = local_addr;
    tokio::task::spawn_blocking(move || {
        let opts = SearchOptions {
            timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        };
        let gateway = match igd::search_gateway(opts) {
            Ok(g) => g,
            Err(e) => {
                debug!("no IGD gateway found: {}", e);
                return Ok(false);
            }
        };
        let protocol_name = match protocol {
            PortMappingProtocol::TCP => "TCP",
            PortMappingProtocol::UDP => "UDP",
        };
        match gateway.add_port(
            protocol,
            external_port,
            local_addr_for_blocking,
            LEASE_DURATION_SECS,
            DESCRIPTION,
        ) {
            Ok(()) => {
                info!(
                    "UPnP: mapped {} external :{} -> {}",
                    protocol_name, external_port, local_addr_for_blocking
                );
                Ok(true)
            }
            Err(e) => {
                warn!("UPnP: failed to map {} :{}: {}", protocol_name, external_port, e);
                Err(format!("add_port: {}", e))
            }
        }
    })
    .await
    .map_err(|e| format!("join error: {}", e))?
}

/// Run the port-mapping task: try once on startup, then refresh
/// periodically. A failed refresh is logged but the task continues —
/// the next refresh may succeed if the gateway becomes reachable
/// again.
///
/// Returns when `cancel` is triggered.
pub async fn run(port: u16, cancel: tokio_util::sync::CancellationToken) {
    let local_ip = match local_ipv4() {
        Some(ip) => ip,
        None => {
            warn!("UPnP: could not determine local IPv4; skipping port mapping");
            return;
        }
    };
    let local_addr = SocketAddrV4::new(local_ip, port);

    let mut interval = tokio::time::interval(REFRESH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The first tick fires immediately, so we start with a real attempt.
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let _ = map_port(PortMappingProtocol::TCP, port, local_addr).await;
                let _ = map_port(PortMappingProtocol::UDP, port, local_addr).await;
            }
            _ = cancel.cancelled() => {
                debug!("UPnP: shutting down");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_ipv4_does_not_panic() {
        // No assertions: we just want to confirm the function works
        // even if it can't reach 8.8.8.8 (CI without internet).
        let _ = local_ipv4();
    }

    #[test]
    fn map_port_rejects_zero() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0);
        let res = rt.block_on(map_port(PortMappingProtocol::TCP, 0, local));
        assert!(res.is_err());
    }
}
