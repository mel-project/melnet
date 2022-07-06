use std::{collections::HashSet, str::FromStr, time::Duration};

use anyhow::Context;
use async_net::SocketAddr;
use async_recursion::async_recursion;
use dashmap::DashMap;
use futures_util::stream::FuturesUnordered;
use smol::stream::StreamExt;
use smol_timeout::TimeoutExt;

fn main() -> anyhow::Result<()> {
    smolscale::block_on(async {
        let args = std::env::args().collect::<Vec<_>>();
        let destination = args
            .get(1)
            .context("must pass in a destination to start exploring")?
            .clone();
        let netname = args.get(2).context("must pass in a netname")?.clone();
        let remote_addr =
            SocketAddr::from_str(&destination).context("cannot parse destination address")?;

        let accum = DashMap::new();
        explore(remote_addr, &netname, &accum).await?;
        println!("digraph G {{");
        for (k, v) in accum {
            for v in v.into_iter().take(4) {
                println!("\"{}\" -> \"{}\";", k, v)
            }
        }
        println!("}}");
        Ok(())
    })
}

#[async_recursion]
async fn explore(
    addr: SocketAddr,
    netname: &str,
    accum: &DashMap<SocketAddr, HashSet<SocketAddr>>,
) -> anyhow::Result<()> {
    if accum.contains_key(&addr) {
        return Ok(());
    }
    let routes: HashSet<SocketAddr> = melnet::request(addr, netname, "get_routes", ())
        .timeout(Duration::from_secs(10))
        .await
        .context("timed out")??;
    eprintln!("found {} routes of {}", routes.len(), addr);
    accum.insert(addr, routes.clone());
    let mut children = FuturesUnordered::new();
    for route in routes {
        children.push(explore(route, netname, accum));
    }
    while let Some(val) = children.next().await {
        if let Err(_err) = val {
            // eprintln!("encountered an error: {:?}", err)
        }
    }
    Ok(())
}
