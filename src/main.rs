use anyhow::{Result, anyhow};

use monoio;
use monoio::io::{
  AsyncBufReadExt, AsyncWriteRentExt, BufReader, OwnedReadHalf, OwnedWriteHalf, Splitable
};
use monoio::net::{TcpListener, TcpStream};
use local_sync::mpsc::bounded::{channel, Tx, Rx};

mod client;
mod channels;
use channels::{Channels, Publisher, Subscription, Publishers};

type Entry = Vec<u8>;

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

fn next_id() -> usize {
  static COUNTER: AtomicUsize = AtomicUsize::new(1);
  return COUNTER.fetch_add(1, Ordering::Relaxed);
}

fn runtime() -> monoio::RuntimeBuilder<monoio::LegacyDriver> {
  monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
}

fn main() {
  let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
  println!("total cores = {}", cores);
  let channels = Arc::new(Channels::new());

  let threads: Vec<_> = (1 .. cores).map(|_i| {
    let tc = channels.clone();
    thread::spawn(|| {
      runtime().build().expect("Failed building the Runtime")
        .block_on(thread(tc));
    })
  }).collect();

  runtime().build().expect("Failed building the Runtime")
    .block_on(thread(channels));

  threads.into_iter().for_each(|t| {
    let _ = t.join();
  });
}

async fn thread(channels: Arc<Channels>) {
  let thread_id = thread::current().id();
  let thread_count = next_id() as u64;
  let _publishers = RefCell::new(Publishers::new(channels.clone()));
  println!("thread {:?} ({})", thread_id, thread_count);
  thread::sleep(Duration::from_secs(1u64 * thread_count));

    let listener = TcpListener::bind("127.0.0.1:8115").unwrap();
    println!("listening");
    loop {
        let incoming = listener.accept().await;
        match incoming {
            Ok((stream, addr)) => {
                println!("accepted a connection from {} on {:?}", addr, thread_id);

                // @todo: use client impl
                // start_client(stream, addr, channel, publishers)

                let (r, w) = stream.into_split();
                let (tx, rx) = channel::<Entry>(100);
                monoio::spawn(echo_in(r, channels.publisher("*")));
                monoio::spawn(relay(channels.subscribe("*"), tx.clone()));
                monoio::spawn(relay(channels.subscribe("oob"), tx.clone()));
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
  mut from: Subscription,
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
  publisher: Publisher
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
        let res = publisher.send(buffer).await;
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
