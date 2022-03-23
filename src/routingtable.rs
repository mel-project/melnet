use std::net::SocketAddr;
use std::time::Instant;
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Default)]
pub struct RoutingTable {
    addr_last_seen: HashMap<SocketAddr, Instant>,
}

impl RoutingTable {
    /// Adds a route to the routing table, asserting that the route is up to date.
    pub fn add_route(&mut self, addr: SocketAddr) {
        log::trace!("add route {}", addr);
        self.clean_up();
        self.addr_last_seen.insert(addr, Instant::now());
    }

    /// Gets the age of a route, if available
    pub fn get_route_age(&self, addr: SocketAddr) -> Option<Duration> {
        self.addr_last_seen.get(&addr).map(|d| d.elapsed())
    }

    /// Cleans up really old routes.
    fn clean_up(&mut self) {
        self.addr_last_seen
            .retain(|_, last_seen| last_seen.elapsed().as_secs() < 3600);
    }

    /// Gets all the routes out
    pub fn to_vec(&self) -> Vec<(SocketAddr, Instant)> {
        self.addr_last_seen.iter().map(|(k, v)| (*k, *v)).collect()
    }
}
