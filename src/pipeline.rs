use std::{convert::Infallible};

use async_net::TcpStream;
use concurrent_queue::ConcurrentQueue;
use futures_util::{future::Shared, Future, FutureExt};
use smol::prelude::*;
use smol::{
    channel::{Receiver, Sender},
    Task,
};

use crate::{read_len_bts, write_len_bts, MelnetError};

/// A fully pipelined TCP req/resp connection.
#[derive(Clone)]
pub struct Pipeline {
    send_req: Sender<(Vec<u8>, Sender<Vec<u8>>)>,
    recv_err: Shared<Task<Result<Infallible, MelnetError>>>,
}

impl Pipeline {
    /// Wraps a Pipeline around the given TCP stream
    pub fn new(stream: TcpStream) -> Self {
        let (send_req, recv_req) = smol::channel::bounded(16);
        let task = smolscale::spawn(pipeline_inner(stream, recv_req));
        Self {
            send_req,
            recv_err: task.shared(),
        }
    }

    /// Does a single request onto the pipeline.
    pub async fn request(&self, req: Vec<u8>) -> Result<Vec<u8>, MelnetError> {
        let (send_resp, recv_resp) = smol::channel::bounded(1);
        let _ = self.send_req.send((req, send_resp)).await;
        let recv_err = self.recv_err.clone();
        async { Ok(uob(recv_resp.recv()).await) }
            .or(async { Err(recv_err.await.unwrap_err()) })
            .await
    }
}

async fn pipeline_inner(
    mut ustream: TcpStream,
    recv_req: Receiver<(Vec<u8>, Sender<Vec<u8>>)>,
) -> Result<Infallible, MelnetError> {
    let queue = ConcurrentQueue::unbounded();
    let mut dstream = ustream.clone();
    let up = async {
        loop {
            let (req, send_resp) = uob(recv_req.recv()).await;
            queue.push(send_resp).unwrap();
            write_len_bts(&mut ustream, &req).await?;
        }
    };
    let down = async {
        loop {
            let resp = read_len_bts(&mut dstream).await?;
            if let Ok(send_resp) = queue.pop() {
                let _ = send_resp.try_send(resp);
            }
        }
    };
    up.race(down).await
}

async fn uob<T, E>(f: impl Future<Output = Result<T, E>>) -> T {
    match f.await {
        Ok(t) => t,
        _ => smol::future::pending().await,
    }
}
