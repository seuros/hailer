//! RFC 8305-style staggered dual-stack TCP dialer so a blackholed IPv6
//! (or unreachable first A/AAAA) record never stalls a dual-stack SMTP relay
//! until the entire send deadline expires.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;

pub(super) async fn connect_tcp(host: &str, port: u16) -> io::Result<TcpStream> {
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
    connect_addrs(resolved).await
}

pub(in crate::provider::smtp) async fn connect_addrs(
    resolved: Vec<SocketAddr>,
) -> io::Result<TcpStream> {
    const STAGGER: Duration = Duration::from_millis(250);

    if resolved.is_empty() {
        return Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "no addresses resolved"));
    }
    let addrs = interleave_addr_families(resolved);
    if addrs.len() == 1 {
        return TcpStream::connect(addrs[0]).await;
    }

    let mut pending = tokio::task::JoinSet::new();
    let mut next = addrs.into_iter();
    let mut last_err = None;

    if let Some(first) = next.next() {
        pending.spawn(async move { TcpStream::connect(first).await });
    }

    loop {
        if let Some(addr) = next.next() {
            match tokio::time::timeout(STAGGER, pending.join_next()).await {
                Ok(Some(Ok(Ok(stream)))) => return Ok(stream),
                Ok(Some(Ok(Err(err)))) => {
                    last_err = Some(err);
                    pending.spawn(async move { TcpStream::connect(addr).await });
                }
                Ok(Some(Err(join_err))) => {
                    last_err = Some(io::Error::other(join_err));
                    pending.spawn(async move { TcpStream::connect(addr).await });
                }
                Ok(None) | Err(_) => {
                    pending.spawn(async move { TcpStream::connect(addr).await });
                }
            }
        } else {
            match pending.join_next().await {
                Some(Ok(Ok(stream))) => return Ok(stream),
                Some(Ok(Err(err))) => last_err = Some(err),
                Some(Err(join_err)) => last_err = Some(io::Error::other(join_err)),
                None => {
                    return Err(last_err.unwrap_or_else(|| {
                        io::Error::new(io::ErrorKind::NotConnected, "failed to connect")
                    }));
                }
            }
        }
    }
}

pub(in crate::provider::smtp) fn interleave_addr_families(
    addrs: Vec<SocketAddr>,
) -> Vec<SocketAddr> {
    let first_is_v6 = addrs.first().is_some_and(SocketAddr::is_ipv6);
    let (primary, secondary): (Vec<_>, Vec<_>) =
        addrs.into_iter().partition(|addr| addr.is_ipv6() == first_is_v6);
    let mut out = Vec::with_capacity(primary.len() + secondary.len());
    let (mut a, mut b) = (primary.into_iter(), secondary.into_iter());
    loop {
        match (a.next(), b.next()) {
            (Some(x), Some(y)) => {
                out.push(x);
                out.push(y);
            }
            (Some(x), None) => out.push(x),
            (None, Some(y)) => out.push(y),
            (None, None) => break,
        }
    }
    out
}
