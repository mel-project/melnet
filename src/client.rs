use crate::reqs::*;
use crate::{common::*, pool_manager::TcpPoolManager};
use dashmap::DashMap;
use deadpool::managed::{Object, Pool, PoolError};
use lazy_static::lazy_static;

use serde::{de::DeserializeOwned, Serialize};

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};

lazy_static! {
    static ref CONN_POOL: Client = Client::default();
}

/// Does a melnet request to any given endpoint, using the global client.
pub async fn request<TInput: Serialize + Clone, TOutput: DeserializeOwned + std::fmt::Debug>(
    addr: SocketAddr,
    netname: &str,
    verb: &str,
    req: TInput,
) -> Result<TOutput> {
    CONN_POOL.request(addr, netname, verb, req).await
}

/// Implements a thread-safe pool of connections to melnet, or any HTTP/1.1-style keepalive protocol, servers.
#[derive(Default)]
pub struct Client {
    pool: DashMap<SocketAddr, deadpool::managed::Pool<TcpPoolManager>>,
}

impl Client {
    /// Connects to a given address, which may return either a new connection or an existing one.
    async fn connect(&self, addr: impl ToSocketAddrs) -> std::io::Result<Object<TcpPoolManager>> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        let existing = self
            .pool
            .entry(addr)
            .or_insert_with(|| {
                Pool::builder(TcpPoolManager(addr))
                    .max_size(64)
                    .build()
                    .unwrap()
            })
            .clone();
        let conn = existing.get().await.map_err(|err| match err {
            PoolError::Timeout(_) => panic!("should never see deadpool timeout"),
            PoolError::Closed => panic!("closed"),
            PoolError::NoRuntimeSpecified => panic!("what"),
            PoolError::Backend(err) => err,
            PoolError::PostCreateHook(_) => todo!(),
            PoolError::PreRecycleHook(_) => todo!(),
            PoolError::PostRecycleHook(_) => todo!(),
        })?;
        Ok(conn)
    }

    /// Does a melnet request to any given endpoint.
    pub async fn request<TInput: Serialize + Clone, TOutput: DeserializeOwned + std::fmt::Debug>(
        &self,
        addr: SocketAddr,
        netname: &str,
        verb: &str,
        req: TInput,
    ) -> Result<TOutput> {
        for count in 0..5 {
            match self.request_inner(addr, netname, verb, req.clone()).await {
                Err(MelnetError::Network(err)) => {
                    log::debug!(
                        "retrying request {} to {} on transient network error {:?}",
                        verb,
                        addr,
                        err
                    );
                    smol::Timer::after(Duration::from_secs_f64(0.1 * 2.0f64.powi(count))).await;
                }
                x => return x,
            }
        }
        self.request_inner(addr, netname, verb, req).await
    }

    async fn request_inner<TInput: Serialize, TOutput: DeserializeOwned + std::fmt::Debug>(
        &self,
        addr: SocketAddr,
        netname: &str,
        verb: &str,
        req: TInput,
    ) -> Result<TOutput> {
        // // Semaphore
        // static GLOBAL_LIMIT: Semaphore = Semaphore::new(128);
        // let _guard = GLOBAL_LIMIT.acquire().await;
        let start = Instant::now();
        // grab a connection
        let mut conn = self.connect(addr).await.map_err(MelnetError::Network)?;
        let res = async {
            // send a request
            let rr = stdcode::serialize(&RawRequest {
                proto_ver: PROTO_VER,
                netname: netname.to_owned(),
                verb: verb.to_owned(),
                payload: stdcode::serialize(&req).unwrap(),
            })
            .unwrap();
            write_len_bts(conn.as_mut().as_mut(), &rr).await?;
            // read the response length
            let response: RawResponse = stdcode::deserialize(
                &read_len_bts(conn.as_mut().as_mut()).await?,
            )
            .map_err(|e| {
                MelnetError::Network(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            let response = match response.kind.as_ref() {
                "Ok" => stdcode::deserialize::<TOutput>(&response.body)
                    .map_err(|_| MelnetError::Custom("stdcode error".to_owned()))?,
                "NoVerb" => return Err(MelnetError::VerbNotFound),
                _ => {
                    return Err(MelnetError::Custom(
                        String::from_utf8_lossy(&response.body).to_string(),
                    ))
                }
            };
            let elapsed = start.elapsed();
            if elapsed.as_secs_f64() > 3.0 {
                log::warn!("melnet req to {} took {:?}", addr, elapsed)
            }
            Ok::<_, crate::MelnetError>(response)
        };
        match res.await {
            Err(err) => {
                Object::take(conn);
                Err(err)
            }
            x => x,
        }
    }
}
