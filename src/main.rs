/// A echo example.
///
/// Run the example and `nc 127.0.0.1 50002` in another shell.
/// All your input will be echoed out.
use monoio::io::{
  Splitable,
  OwnedReadHalf,
  AsyncReadRent,
  OwnedWriteHalf,
  AsyncWriteRentExt,
};
use monoio::net::{TcpListener, TcpStream};
use async_broadcast::{broadcast, Sender, Receiver};

#[monoio::main]
async fn main() {
    let (mut send, recv) = broadcast::<Vec<u8>>(5);
    send.set_overflow(true);
    let recv = recv.deactivate();
    let listener = TcpListener::bind("127.0.0.1:50002").unwrap();
    println!("listening");
    loop {
        let incoming = listener.accept().await;
        match incoming {
            Ok((stream, addr)) => {
                println!("accepted a connection from {}", addr);
                let (r, w) = stream.into_split();
                monoio::spawn(echo_in(r, send.clone()));
                monoio::spawn(echo_out(w, recv.activate_cloned()));
            }
            Err(e) => {
                println!("accepted connection failed: {}", e);
                return;
            }
        }
    }
}

async fn echo_in(
  mut stream: OwnedReadHalf<TcpStream>,
  send: Sender<Vec<u8>>
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut res;
    loop {
        // read
        (res, buf) = stream.read(buf).await;
        if res? == 0 {
            return Ok(());
        }

        println!("in {:?} from {:?}", buf, stream.peer_addr());

        // broadcast
        let res = send.broadcast(buf.clone()).await;
        if res.is_err() {
          println!("problem broadcasting");
        }

        // clear
        buf.clear();
    }
}

async fn echo_out(
  mut stream: OwnedWriteHalf<TcpStream>,
  mut recv: Receiver<Vec<u8>>
) -> std::io::Result<()> {
    loop {
        // wait for input from socket or broadcast channel
        if let Ok(buf) = recv.recv().await {
          println!("out {:?} to {:?}", buf, stream.peer_addr());
          // write all
          let (res, mut buf) = stream.write_all(buf).await;
          res?;

          // clear
          buf.clear();
        }
    }
}
