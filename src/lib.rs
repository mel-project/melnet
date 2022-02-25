//! Melnet serves as Themelio's peer-to-peer network layer, based on a randomized topology and gossip. Peers are divided into servers, which have a publicly reachable address, and clients, which do not. It's based on a simple stdcode request-response protocol, where the only way to "push" a message is to send a request to a server. There is no multiplexing --- the whole thing works like HTTP/1.1. TCP connections are pretty cheap these days.
//!
//! This also means that clients never receive notifications, and must poll servers.
//!
//! The general way to use `melnet` is as follows:
//!
//! 1. Create a `NetState`. This holds the routing table, RPC verb handlers, and other "global" data.
//! 2. If running as a server, register RPC verbs with `NetState::register_verb` and run `NetState::run_server` in the background.
//! 3. Use a `Client`, like the global one returned by `g_client()`, to make RPC calls to other servers. Servers are simply identified by a `std::net::SocketAddr`.

mod client;
mod endpoint;
mod pool_manager;
mod routingtable;
use anyhow::Context;
use dashmap::DashMap;
use derivative::*;
pub use endpoint::*;
use once_cell::sync::Lazy;
use routingtable::*;
use serde::{de::DeserializeOwned, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tap::TapFallible;
mod reqs;
use async_net::{TcpListener, TcpStream};
mod common;
pub use client::request;
pub use common::*;
use parking_lot::RwLock;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use reqs::*;
use smol::prelude::*;
use smol::Timer;
use smol_timeout::TimeoutExt;
use std::time::Duration;

#[derive(Derivative, Clone, Default)]
#[derivative(Debug)]
/// A clonable structure representing a melnet state. All copies share the same routing table.
pub struct NetState {
    network_name: String,
    routes: Arc<RwLock<RoutingTable>>,
    #[derivative(Debug = "ignore")]
    verbs: Arc<DashMap<String, BoxedResponder>>,
}

impl NetState {
    /// Runs the netstate. Usually you would want to call this in a separate task. This doesn't consume the netstate because the netstate struct can still be used to get out routes, register new verbs, etc even when it's concurrently run as a server.
    pub async fn run_server(&self, listener: TcpListener) {
        let mut this = self.clone();
        this.setup_routing();
        // Spam neighbors with random routes
        let spammer = self.new_addr_spam().race(self.get_routes_spam());

        // Max number of connections
        spammer
            .race(async move {
                loop {
                    let (conn, addr) = listener.accept().await.unwrap();
                    let self_copy = self.clone();
                    // spawn a task, moving the sem_guard inside
                    smolscale::spawn(async move {
                        if let Some(Err(e)) = self_copy
                            .server_handle(conn)
                            .timeout(Duration::from_secs(120))
                            .await
                        {
                            log::debug!("{} terminating on error: {:?}", addr, e)
                        }
                    })
                    .detach();
                }
            })
            .await
    }

    /// Random spammer
    async fn new_addr_spam(&self) {
        let mut rng = rand::rngs::OsRng {};
        let mut tmr = Timer::interval(Duration::from_secs(30));
        loop {
            tmr.next().await;
            let routes = self.routes.read().to_vec();
            if !routes.is_empty() {
                let (rand_neigh, _) = routes[rng.gen::<usize>() % routes.len()];
                let (rand_route, _) = routes[rng.gen::<usize>() % routes.len()];
                let network_name = self.network_name.clone();
                smolscale::spawn(async move {
                    let _ = crate::request::<RoutingRequest, String>(
                        rand_neigh,
                        &network_name,
                        "new_addr",
                        RoutingRequest {
                            proto: String::from("tcp"),
                            addr: rand_route.to_string(),
                        },
                    )
                    .await
                    .tap_err(|err| log::debug!("addrspam failed to {} ({:?})", rand_neigh, err));
                })
                .detach();
            }
        }
    }

    /// Get-routes spam
    async fn get_routes_spam(&self) {
        let mut tmr = Timer::interval(Duration::from_secs(10));
        loop {
            if let Some(route) = self.routes().get(0).copied() {
                let network_name = self.network_name.clone();
                let state = self.clone();
                smolscale::spawn(async move {
                    let resp: Vec<SocketAddr> = crate::request::<(), Vec<SocketAddr>>(
                        route,
                        &network_name,
                        "get_routes",
                        (),
                    )
                    .timeout(Duration::from_secs(10))
                    .await
                    .context("timeout")
                    .tap_err(|err| {
                        log::debug!("could not get routes from {}: {:?}", route, err)
                    })??;
                    log::debug!("{} routes from {}", resp.len(), route);
                    for new_route in resp {
                        log::debug!("testing {}", new_route);
                        let state = state.clone();
                        let network_name = network_name.clone();
                        smolscale::spawn(async move {
                            crate::request::<_, u64>(new_route, &network_name, "ping", 10)
                                .timeout(Duration::from_secs(10))
                                .await
                                .context("timeout")
                                .tap_err(|err| {
                                    log::warn!(
                                        "route {} from {} was unpingable ({:?})!",
                                        new_route,
                                        route,
                                        err
                                    )
                                })??;
                            state.add_route(new_route);
                            Ok::<_, anyhow::Error>(())
                        })
                        .detach();
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .detach();
            }
            tmr.next().await;
        }
    }

    async fn server_handle(&self, mut conn: TcpStream) -> anyhow::Result<()> {
        conn.set_nodelay(true)?;
        loop {
            self.server_handle_one(&mut conn).await?;
        }
    }

    async fn server_handle_one(&self, conn: &mut TcpStream) -> anyhow::Result<()> {
        // read command
        let cmd: RawRequest = stdcode::deserialize(&read_len_bts(conn.clone()).await?)?;
        if cmd.proto_ver != 1 {
            let err = stdcode::serialize(&RawResponse {
                kind: "Err".to_owned(),
                body: stdcode::serialize(&"bad protocol version").unwrap(),
            })
            .unwrap();
            write_len_bts(conn, &err).await?;
            return Err(anyhow::anyhow!("bad"));
        }
        if cmd.netname != self.network_name {
            return Err(anyhow::anyhow!("bad"));
        }
        log::trace!("got command {:?} from {:?}", cmd, conn.peer_addr());
        // respond to command
        let response_fut = {
            let responder = self.verbs.get(&cmd.verb);
            if let Some(responder) = responder {
                let res = responder.0(&cmd.payload);
                Some(res)
            } else {
                None
            }
        };
        let response: Result<Vec<u8>> = if let Some(fut) = response_fut {
            fut.await
        } else {
            Err(MelnetError::VerbNotFound)
        };
        match response {
            Ok(resp) => {
                write_len_bts(
                    conn,
                    &stdcode::serialize(&RawResponse {
                        kind: "Ok".into(),
                        body: resp,
                    })
                    .unwrap(),
                )
                .await?
            }
            Err(MelnetError::Custom(string)) => {
                write_len_bts(
                    conn,
                    &stdcode::serialize(&RawResponse {
                        kind: "Err".into(),
                        body: string.as_bytes().into(),
                    })
                    .unwrap(),
                )
                .await?
            }
            Err(MelnetError::VerbNotFound) => {
                write_len_bts(
                    conn,
                    &stdcode::serialize(&RawResponse {
                        kind: "NoVerb".into(),
                        body: b"".to_vec(),
                    })
                    .unwrap(),
                )
                .await?
            }
            err => {
                log::error!(
                    "bad error created by responder at verb {}: {:?}",
                    cmd.verb,
                    err
                );
                anyhow::bail!("wtf")
            }
        }
        Ok(())
    }

    /// Registers the handler for new_peer.
    fn setup_routing(&mut self) {
        // ping just responds to a u64 with itself
        self.listen("ping", |ping: Request<u64>| async move {
            let body = ping.body;
            Ok(body)
        });
        // new_addr adds a new address
        self.listen("new_addr", |request: Request<RoutingRequest>| async move {
            let rr = request.body.clone();
            let state = request.state.clone();
            if rr.proto != "tcp" {
                log::debug!("new_addr saw unrecognizable protocol = {:?}", rr.proto);
                anyhow::bail!("bad protocol")
            }
            // move into a task now
            let their_addr = *smol::net::resolve(&rr.addr)
                .await
                .context("cannot resolve address given")?
                .first()
                .context("zero addresses in the hostname given")?;
            let resp: u64 =
                crate::request(their_addr, &state.network_name.to_owned(), "ping", 814u64)
                    .await
                    .tap_err(|err| log::warn!("error while pinging {}: {:?}", their_addr, err))
                    .context("remote was unpingable")?;
            if resp != 814 {
                log::debug!("new_addr bad ping {:?} {:?}", rr.addr, resp);
                anyhow::bail!("remote responded to ping corruptly")
            } else {
                let prev_routes = state.routes().len();
                state.add_route(their_addr);
                let new_routes = state.routes().len();
                if new_routes > prev_routes {
                    log::debug!("received route {}; now {} routes", their_addr, new_routes);
                }
                Ok::<String, anyhow::Error>("".into())
            }
        });
        // get_routes dumps out a slice of known routes
        self.listen("get_routes", |request: Request<()>| async move {
            Ok(request.state.routes())
        })
    }

    /// Registers a verb.
    pub fn listen<
        Req: DeserializeOwned + Send + 'static,
        Resp: Serialize + Send + 'static,
        T: Endpoint<Req, Resp> + Send + 'static,
    >(
        &self,
        verb: &str,
        responder: T,
    ) {
        let responder = responder_to_closure(self.clone(), responder);
        self.verbs.insert(verb.into(), responder);
    }

    /// Adds a route to the routing table.
    pub fn add_route(&self, addr: SocketAddr) {
        self.routes.write().add_route(addr)
    }

    /// Obtains a vector of routes. This is guaranteed to be uniformly shuffled, so taking the first N elements is always fair.
    pub fn routes(&self) -> Vec<SocketAddr> {
        let mut rr: Vec<SocketAddr> = self.routes.read().to_vec().iter().map(|v| v.0).collect();
        rr.shuffle(&mut thread_rng());
        rr
    }

    /// Sets the name of the network state.
    fn set_name(&mut self, name: &str) {
        self.network_name = name.to_string()
    }

    /// Constructs a netstate with a given name.
    pub fn new_with_name(name: &str) -> Self {
        let mut ns = NetState::default();
        ns.set_name(name);
        ns
    }
}
