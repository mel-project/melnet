use crate::{common::*, pipeline::Pipeline};

use crate::reqs::*;

use async_net::TcpStream;
use dashmap::DashMap;
use lazy_static::lazy_static;

use serde::{de::DeserializeOwned, Serialize};
use smol::lock::Semaphore;
use smol_timeout::TimeoutExt;

use std::net::SocketAddr;
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
    match CONN_POOL
        .request(addr, netname, verb, req)
        .timeout(Duration::from_secs(60))
        .await
    {
        Some(v) => v,
        None => Err(MelnetError::Network(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "long timeout at 60 seconds",
        ))),
    }
}

const POOL_SIZE: usize = 4;

/// Implements a thread-safe pool of connections to melnet, or any HTTP/1.1-style keepalive protocol, servers.
#[derive(Default)]
pub struct Client {
    pool: [DashMap<SocketAddr, (Pipeline, Instant)>; POOL_SIZE],
}

impl Client {
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
        static GLOBAL_LIMIT: Semaphore = Semaphore::new(256);
        let start = Instant::now();
        let _guard = GLOBAL_LIMIT.acquire().await;
        log::debug!("acquired semaphore by {:?}", start.elapsed());
        let start = Instant::now();
        let pool = &self.pool[fastrand::usize(0..self.pool.len())];
        let conn = if let Some(v) = pool.get(&addr).filter(|d| d.1.elapsed().as_secs() < 60) {
            v.0.clone()
        } else {
            let t = TcpStream::connect(addr)
                .await
                .map_err(MelnetError::Network)?;
            let pipe = Pipeline::new(t);
            pool.insert(addr, (pipe.clone(), Instant::now()));
            pipe
        };
        log::debug!("acquired connection by {:?}", start.elapsed());

        let res = async {
            // send a request
            let rr = stdcode::serialize(&RawRequest {
                proto_ver: PROTO_VER,
                netname: netname.to_owned(),
                verb: verb.to_owned(),
                payload: stdcode::serialize(&req).unwrap(),
            })
            .unwrap();
            // read the response length
            let response: RawResponse =
                stdcode::deserialize(&conn.request(rr).await?).map_err(|e| {
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
                log::warn!(
                    "melnet req of verb {}/{} to {} took {:?}",
                    netname,
                    verb,
                    addr,
                    elapsed
                )
            }
            Ok::<_, crate::MelnetError>(response)
        };
        match res.await {
            Ok(v) => Ok(v),
            Err(err) => {
                pool.remove(&addr);
                Err(err)
            }
        }
    }
}
