
use anyhow::{Result, anyhow};

use monoio::io::{
  AsyncBufReadExt, AsyncWriteRentExt, BufReader, OwnedReadHalf, OwnedWriteHalf, Splitable
};
use monoio::net::{TcpListener, TcpStream};
use local_sync::mpsc::bounded::{channel, Tx, Rx};
use async_broadcast::{broadcast, Sender, Receiver};

type Entry = Vec<u8>;

mod channels;
mod client;

#[monoio::main]
async fn main() {
    let (mut _s1, r1) = broadcast::<Entry>(5);
    let (mut send, recv) = broadcast::<Entry>(5);
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
                let (tx, rx) = channel::<Entry>(100);
                monoio::spawn(echo_in(r, send.clone()));
                monoio::spawn(relay(recv.activate_cloned(), tx.clone()));
                monoio::spawn(relay(r1.clone(), tx.clone()));
                monoio::spawn(echo_out(w, rx));
            }
            Err(e) => {
                println!("accepted connection failed: {}", e);
                return;
            }
        }
    }
}

async fn relay(
  mut from: Receiver<Entry>,
  to: Tx<Entry>
) -> Result<()> {
  loop {
      // relay messages to local transit channel
      let msg = from.recv().await?;
      to.send(msg).await
        .map_err(|err| { anyhow!("{:?}", err) })?;
  }
}

async fn echo_in(
  stream: OwnedReadHalf<TcpStream>,
  send: Sender<Entry>
) -> std::io::Result<()> {
    let addr = stream.peer_addr();
    let mut reader = BufReader::new(stream);
    loop {
        // read line
        let mut buffer = Vec::new();
        let res = reader.read_until(b'\n', &mut buffer).await;
        let count = res?;
        if count == 0 {
            return Ok(());
        }

        println!("in {} {:?} from {:?}", count, buffer, addr);

        // broadcast
        let res = send.broadcast(buffer).await;
        if res.is_err() {
          println!("problem broadcasting");
        }
    }
}

async fn echo_out(
  mut stream: OwnedWriteHalf<TcpStream>,
  mut rx: Rx<Entry>
) -> std::io::Result<()> {
    let addr = stream.peer_addr();
    loop {
        // wait for input from socket or broadcast channel
        if let Some(msg) = rx.recv().await {
          println!("out {:?} to {:?}", msg, addr);
          // write all
          let (res, _) = stream.write_all(msg).await;
          res?;
        }
    }
}
