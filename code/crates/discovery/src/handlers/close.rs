use libp2p::{swarm::ConnectionId, PeerId, Swarm};
use tracing::{debug, warn};

use crate::{Discovery, DiscoveryClient, State};

impl<C> Discovery<C>
where
    C: DiscoveryClient,
{
    pub fn can_close(&mut self) -> bool {
        self.state == State::Idle && self.controller.close.can_perform()
    }

    fn should_close(&self, peer_id: PeerId, connection_id: ConnectionId) -> bool {
        // Only close ephemeral connections (i.e not inbound/outbound connections)
        // NOTE: a inbound or outbound connection can still be closed if it is not
        // part of the active connections to the peer. This is possible due to the
        // limit of the number of connections per peer.
        (!self.outbound_peers.contains_key(&peer_id) && !self.inbound_peers.contains(&peer_id))
            || self
                .active_connections
                .get(&peer_id)
                .is_none_or(|connection_ids| !connection_ids.contains(&connection_id))
    }

    pub fn close_connection(
        &mut self,
        swarm: &mut Swarm<C>,
        peer_id: PeerId,
        connection_id: ConnectionId,
    ) {
        if !self.should_close(peer_id, connection_id) {
            return;
        }

        debug!("Closing connection {connection_id} to peer {peer_id}");
        // Close the connection even if it is not active
        swarm.close_connection(connection_id);
    }

    pub fn handle_closed_connection(
        &mut self,
        swarm: &mut Swarm<C>,
        peer_id: PeerId,
        connection_id: ConnectionId,
    ) {
        // Transport connections are tracked before Identify admits them as active.
        self.connections.remove(&connection_id);

        let mut was_last_connection = false;

        if let Some(connection_ids) = self.active_connections.get_mut(&peer_id) {
            if connection_ids.contains(&connection_id) {
                warn!("Removing active connection {connection_id} to peer {peer_id}");
                connection_ids.retain(|id| id != &connection_id);

                if connection_ids.is_empty() {
                    self.active_connections.remove(&peer_id);

                    was_last_connection = true;
                }
            } else {
                warn!("Non-established connection {connection_id} to peer {peer_id} closed");
            }
        }

        // In case the connection was closed before identifying the peer
        self.controller.dial.remove_in_progress(&connection_id);

        if self.outbound_peers.contains_key(&peer_id) {
            warn!("Outbound connection {connection_id} to peer {peer_id} closed");

            if was_last_connection {
                warn!("Last connection to peer {peer_id} closed, removing from outbound peers");

                self.outbound_peers.remove(&peer_id);
            }

            if self.is_enabled() {
                self.repair_outbound_peers(swarm);
            }
        } else if self.inbound_peers.contains(&peer_id) {
            warn!("Inbound connection {connection_id} to peer {peer_id} closed");

            if was_last_connection {
                warn!("Last connection to peer {peer_id} closed, removing from inbound peers");

                self.inbound_peers.remove(&peer_id);
            }
        }

        // Clean up discovered peers when all connections are closed
        if was_last_connection {
            self.cleanup_peer_on_disconnect(peer_id);
        }

        self.update_discovery_metrics();
    }

    /// Clean up peer state and dial history when the last connection to a peer is closed
    fn cleanup_peer_on_disconnect(&mut self, peer_id: PeerId) {
        let peer_info = self.discovered_peers.remove(&peer_id);

        // Remove signed peer record (no longer connected, record may be stale)
        self.signed_peer_records.remove(&peer_id);

        // Clear rate limiter state for this peer
        self.rate_limiter.remove_peer(&peer_id);

        // Clear connect_request done_on to allow re-upgrading the peer on reconnection
        self.controller.connect_request.remove_done_on(&peer_id);

        // Find and reset the bootstrap node peer_id to allow re-identification
        // This handles the case where a bootstrap node restarts with a different peer_id
        for bootstrap_node in self.bootstrap_nodes.iter_mut() {
            if bootstrap_node.0 == Some(peer_id) {
                warn!(
                    "Resetting bootstrap node peer_id {} to allow re-identification",
                    peer_id
                );
                bootstrap_node.0 = None; // Reset to None so it can be re-identified
                self.controller
                    .dial_clear_done_for_peer(peer_id, &bootstrap_node.1);
                return;
            }
        }

        // Handle non-bootstrap peers when discovery is disabled
        if !self.is_enabled() {
            let addrs = peer_info.map(|info| info.listen_addrs).unwrap_or_default();
            self.controller.dial_clear_done_for_peer(peer_id, &addrs);
        }
    }
}

#[cfg(test)]
mod tests {
    use libp2p::core::{
        transport::dummy::DummyTransport, transport::PortUse, ConnectedPoint, Endpoint,
    };
    use libp2p::identity::Keypair;
    use libp2p::kad::{Addresses, KBucketKey, KBucketRef, RoutingUpdate};
    use libp2p::request_response::{OutboundRequestId, ResponseChannel};
    use libp2p::swarm::{dummy, Config as SwarmConfig};
    use libp2p::{identify, Multiaddr, Transport};
    use malachitebft_metrics::Registry;

    use super::*;
    use crate::{Config, DialData, Request, Response};

    // These tests drive the discovery handlers directly, without networking.
    impl DiscoveryClient for dummy::Behaviour {
        fn add_address(&mut self, _: &PeerId, _: Multiaddr) -> RoutingUpdate {
            unreachable!()
        }

        fn kbuckets(
            &mut self,
        ) -> impl Iterator<Item = KBucketRef<'_, KBucketKey<PeerId>, Addresses>> {
            std::iter::empty()
        }

        fn send_request(&mut self, _: &PeerId, _: Request) -> OutboundRequestId {
            unreachable!()
        }

        fn send_response(
            &mut self,
            _: ResponseChannel<Response>,
            _: Response,
        ) -> Result<(), Response> {
            unreachable!()
        }
    }

    fn fixture(config: Config) -> (Discovery<dummy::Behaviour>, Swarm<dummy::Behaviour>) {
        let local_key = Keypair::ed25519_from_bytes([1; 32]).unwrap();
        let swarm = Swarm::new(
            DummyTransport::new().boxed(),
            dummy::Behaviour,
            local_key.public().to_peer_id(),
            SwarmConfig::without_executor(),
        );
        let discovery = Discovery::new(config, vec![], &mut Registry::default());
        (discovery, swarm)
    }

    fn peer_info() -> (PeerId, identify::Info) {
        let public_key = Keypair::ed25519_from_bytes([2; 32]).unwrap().public();
        let peer_id = public_key.to_peer_id();
        let info = identify::Info {
            public_key,
            protocol_version: "/consensus/test".to_owned(),
            agent_version: "discovery-test".to_owned(),
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/10001".parse().unwrap()],
            protocols: vec![],
            observed_addr: "/ip4/127.0.0.1/tcp/10000".parse().unwrap(),
            signed_peer_record: None,
        };
        (peer_id, info)
    }

    fn inbound_endpoint(info: &identify::Info) -> ConnectedPoint {
        ConnectedPoint::Listener {
            local_addr: info.observed_addr.clone(),
            send_back_addr: info.listen_addrs[0].clone(),
        }
    }

    #[test]
    fn closing_before_identify_cleans_up_transport_and_pending_dial() {
        let (mut discovery, mut swarm) = fixture(Config::new(false));
        let (peer_id, info) = peer_info();
        let connection_id = ConnectionId::new_unchecked(1);
        discovery.controller.dial.register_in_progress(
            connection_id,
            DialData::new(Some(peer_id), info.listen_addrs.clone()),
        );
        discovery.handle_connection(
            &mut swarm,
            peer_id,
            connection_id,
            ConnectedPoint::Dialer {
                address: info.listen_addrs[0].clone(),
                role_override: Endpoint::Dialer,
                port_use: PortUse::Reuse,
            },
        );
        assert!(discovery.connections.contains_key(&connection_id));
        assert!(discovery.controller.dial.is_in_progress(&connection_id));

        discovery.handle_closed_connection(&mut swarm, peer_id, connection_id);

        assert!(!discovery.connections.contains_key(&connection_id));
        assert!(!discovery.controller.dial.is_in_progress(&connection_id));
        assert!(!discovery.active_connections.contains_key(&peer_id));
        assert!(!discovery.discovered_peers.contains_key(&peer_id));
    }

    #[test]
    fn closing_peer_rejected_at_identify_cleans_up_transport() {
        let mut config = Config::new(false);
        config.set_persistent_peers_only(true);
        let (mut discovery, mut swarm) = fixture(config);
        let (peer_id, info) = peer_info();
        let connection_id = ConnectionId::new_unchecked(1);
        discovery.handle_connection(&mut swarm, peer_id, connection_id, inbound_endpoint(&info));
        assert!(discovery.handle_new_peer(&mut swarm, connection_id, peer_id, info));
        assert!(discovery.connections.contains_key(&connection_id));
        assert!(!discovery.active_connections.contains_key(&peer_id));

        discovery.handle_closed_connection(&mut swarm, peer_id, connection_id);

        assert!(!discovery.connections.contains_key(&connection_id));
        assert!(!discovery.is_inbound_peer(&peer_id));
        assert!(!discovery.discovered_peers.contains_key(&peer_id));
    }

    #[test]
    fn closing_connection_above_peer_limit_preserves_active_peer() {
        let mut config = Config::new(false);
        config.set_max_connections_per_peer(1);
        let (mut discovery, mut swarm) = fixture(config);
        let (peer_id, info) = peer_info();
        let active_id = ConnectionId::new_unchecked(1);
        let excess_id = ConnectionId::new_unchecked(2);
        discovery.handle_connection(&mut swarm, peer_id, active_id, inbound_endpoint(&info));
        assert!(!discovery.handle_new_peer(&mut swarm, active_id, peer_id, info.clone()));
        discovery.handle_connection(&mut swarm, peer_id, excess_id, inbound_endpoint(&info));
        assert!(discovery.handle_new_peer(&mut swarm, excess_id, peer_id, info));
        assert_eq!(discovery.active_connections[&peer_id], [active_id]);
        assert!(discovery.connections.contains_key(&excess_id));

        discovery.handle_closed_connection(&mut swarm, peer_id, excess_id);

        assert!(!discovery.connections.contains_key(&excess_id));
        assert!(discovery.connections.contains_key(&active_id));
        assert_eq!(discovery.active_connections[&peer_id], [active_id]);
        assert!(discovery.is_inbound_peer(&peer_id));
        assert!(discovery.discovered_peers.contains_key(&peer_id));

        discovery.handle_closed_connection(&mut swarm, peer_id, active_id);

        assert!(discovery.connections.is_empty());
        assert!(!discovery.active_connections.contains_key(&peer_id));
        assert!(!discovery.is_inbound_peer(&peer_id));
        assert!(!discovery.discovered_peers.contains_key(&peer_id));
    }
}
