use async_net::{SocketAddr, TcpStream};

/// A very simple connection pool.
pub struct TcpPoolManager(pub SocketAddr);

#[async_trait::async_trait]
impl deadpool::managed::Manager for TcpPoolManager {
    type Type = TcpStream;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = TcpStream::connect(&self.0).await?;
        conn.set_nodelay(true)?;
        Ok(conn)
    }

    async fn recycle(
        &self,
        _conn: &mut Self::Type,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        Ok(())
    }
}
