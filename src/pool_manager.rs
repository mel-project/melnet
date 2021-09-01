use std::time::{Duration, Instant};

use async_net::{SocketAddr, TcpStream};
use deadpool::managed::RecycleError;

/// A very simple connection pool.
pub struct TcpPoolManager(pub SocketAddr);

#[derive(Clone, Debug)]
pub struct TempTcpStream {
    tcp: TcpStream,
    deadline: Instant,
}

impl std::ops::Deref for TempTcpStream {
    type Target = TcpStream;
    fn deref(&self) -> &Self::Target {
        &self.tcp
    }
}

impl std::ops::DerefMut for TempTcpStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tcp
    }
}

impl AsRef<TcpStream> for TempTcpStream {
    fn as_ref(&self) -> &TcpStream {
        &self.tcp
    }
}

impl AsMut<TcpStream> for TempTcpStream {
    fn as_mut(&mut self) -> &mut TcpStream {
        &mut self.tcp
    }
}

impl From<TcpStream> for TempTcpStream {
    fn from(t: TcpStream) -> Self {
        Self {
            tcp: t,
            deadline: Instant::now() + Duration::from_secs(5),
        }
    }
}

#[async_trait::async_trait]
impl deadpool::managed::Manager for TcpPoolManager {
    type Type = TempTcpStream;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = TcpStream::connect(&self.0).await?;
        conn.set_nodelay(true)?;
        Ok(conn.into())
    }

    async fn recycle(
        &self,
        conn: &mut Self::Type,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        if conn.deadline < Instant::now() {
            Err(RecycleError::Message("connection too old".into()))
        } else {
            Ok(())
        }
    }
}
