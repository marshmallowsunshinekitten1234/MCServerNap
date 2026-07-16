use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};

const FIRST_LISTENER_PORT: u16 = 29_000;
const PORTS_PER_NAMESPACE: u16 = 8;

static NEXT_TCP_NAMESPACE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn allocate_tcp_namespace() -> u8 {
    let previous = NEXT_TCP_NAMESPACE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .expect("test TCP namespace counter should not overflow");
    u8::try_from(previous + 1).expect("tests should not exhaust isolated TCP namespaces")
}

pub(crate) fn tcp_address(namespace: u8) -> Ipv4Addr {
    assert_ne!(namespace, 0, "test TCP namespace must be nonzero");
    Ipv4Addr::new(127, 0, 1, namespace)
}

pub(crate) fn tcp_addresses<const N: usize>(namespace: u8) -> [SocketAddr; N] {
    assert!(
        N <= usize::from(PORTS_PER_NAMESPACE),
        "test TCP namespace has only {PORTS_PER_NAMESPACE} ports"
    );
    let address = tcp_address(namespace);
    let port_offset = (u16::from(namespace) - 1)
        .checked_mul(PORTS_PER_NAMESPACE)
        .expect("test TCP port offset should be representable");
    let first_port = FIRST_LISTENER_PORT
        .checked_add(port_offset)
        .expect("tests should not exhaust isolated TCP ports");
    std::array::from_fn(|index| {
        let port = first_port
            .checked_add(u16::try_from(index).expect("listener index should fit in u16"))
            .expect("test TCP listener port should be representable");
        SocketAddr::from((address, port))
    })
}
