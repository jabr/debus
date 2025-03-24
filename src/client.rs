use core::net::{SocketAddr};
use monoio::net::{TcpStream};

struct Client {
  net: TcpStream,
  addr: SocketAddr,
}
