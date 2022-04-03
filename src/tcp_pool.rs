use async_net::{SocketAddr, TcpStream};
use smol::channel::{Receiver, Sender};
use smol::prelude::*;
use std::time::Duration;

/// A pool of TCP connections
pub struct TcpPool {
    send_conn: Sender<TcpStream>,
    recv_conn: Receiver<TcpStream>,
    timeout: Duration,
    destination: SocketAddr,
}

impl TcpPool {
    /// Creates a new TcpPool, with a given max size and timeout
    pub fn new(max_size: usize, timeout: Duration, destination: SocketAddr) -> Self {
        let (send_conn, recv_conn) = smol::channel::bounded(max_size);
        TcpPool {
            send_conn,
            recv_conn,
            timeout,
            destination,
        }
    }

    /// Gets a connection from the pool, or fails if the pool is empty.
    pub async fn connect(&self) -> std::io::Result<TcpStream> {
        let in_pool = async { Ok(self.recv_conn.recv().await.unwrap()) };
        let new_conn = async { TcpStream::connect(self.destination).await };
        in_pool.or(new_conn).await
    }

    /// Returns this TCP connection to the pool.
    pub fn replenish(&self, conn: TcpStream) {
        if self.send_conn.try_send(conn).is_ok() {
            let recv_conn = self.recv_conn.clone();
            let timeout = self.timeout;
            smolscale::spawn(async move {
                smol::Timer::after(timeout).await;
                let _ = recv_conn.try_recv();
            })
            .detach();
        }
    }
}
