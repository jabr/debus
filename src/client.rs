use super::{Entry, channels::{Channels, Subscription, Publishers}};

use core::net::SocketAddr;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::{AbortHandle, Abortable};
use anyhow::Result;
use monoio::net::TcpStream;
use monoio::io::{
  AsyncBufReadExt,
  AsyncWriteRentExt,
  BufReader,
  OwnedReadHalf,
  OwnedWriteHalf,
  Splitable
};
use local_sync::mpsc::bounded::{channel, Tx, Rx};

fn next_id() -> usize {
  println!("next_id called");
  static COUNTER: AtomicUsize = AtomicUsize::new(1);
  return COUNTER.fetch_add(1, Ordering::Relaxed);
}

struct Client {
  channels: Arc<Channels>,
  publishers: Rc<RefCell<Publishers>>,
  id: usize,
  addr: SocketAddr,
  stream: BufReader<OwnedReadHalf<TcpStream>>,
  out: Tx<Entry>,
  subscriptions: HashMap<String, AbortHandle>,
}

impl Client {
  fn subscribe(&mut self, name: &str) {
    let subscription = self.channels.subscribe(name);
    let id = self.id;
    let out = self.out.clone();
    let (handler, registration) = AbortHandle::new_pair();
    monoio::spawn(async move {
      Abortable::new(
        relay(id, subscription, out),
        registration
      ).await
    });
    self.subscriptions.insert(name.to_string(), handler);
  }

  fn unsubscribe(&mut self, name: &str) {
    if let Some(handler) = self.subscriptions.remove(name) {
      handler.abort();
    }
  }

  async fn publish(&self, name: &str, data: Entry) -> Result<()> {
    return self.publishers.borrow_mut()
      .channel(name).send(data).await;
  }

  async fn serve(&mut self) {
    loop {
      // read line
      let mut buffer = Vec::new();
      if let Ok(count) = self.stream.read_until(b'\n', &mut buffer).await {
        if count == 0 { break; }
        // println!("in {} {:?} from {:?}", count, buffer, self.addr);

        if buffer.starts_with(&[b'+', b'-', b'-']) {
          println!("{:} subscribing to q/*", self.id);
          self.subscribe("q/*");
          continue;
        }

        if buffer.starts_with(&[b'-', b'-', b'-']) {
          println!("{:} unsubscribing to q/*", self.id);
          self.unsubscribe("q/*");
          continue;
        }

        // broadcast
        let res = self.publish("q/*", buffer).await;
        if res.is_err() {
          println!("problem broadcasting {:} {:?}", self.id, res);
        }
      } else { break; }
    }
  }
}

pub fn start_client(
  stream: TcpStream,
  addr: SocketAddr,
  channels: Arc<Channels>,
  publishers: Rc<RefCell<Publishers>>
) {
  let (stream_read, stream_write) = stream.into_split();
  let (channel_tx, channel_rx) = channel::<Entry>(100);
  let id = next_id();

  monoio::spawn(async move {
    pipe_out(id, channel_rx, stream_write).await;
    println!("{:} returned from pipe_out", id);
  });

  monoio::spawn(async move {
    let mut client = Client {
      id,
      addr,
      stream: BufReader::new(stream_read),
      out: channel_tx,
      subscriptions: HashMap::new(),
      channels, publishers,
    };

    client.serve().await;
  });
}

async fn relay(
  id: usize,
  mut from: Subscription,
  to: Tx<Entry>
) -> () {
  // @todo: notify on error?
  loop {
    if let Ok(msg) = from.recv().await {
      let _res = to.send(msg).await;
      if _res.is_err() {
        println!("{:} relay to error {:?}", id, _res);
      }
      // @todo: check res for error other than full channel
      if to.is_closed() {
        println!("{:} relay to closed", id);
        break;
      }
    } else if from.is_closed() {
      println!("{:} relay from closed", id);
      break;
    }
  }
  println!("{:} relay exiting", id);
  drop(from);
  drop(to);
}

async fn pipe_out(
  id: usize,
  mut rx: Rx<Entry>,
  mut stream: OwnedWriteHalf<TcpStream>
) -> () {
  let info = format!("[{:}]\n", id).into_bytes();
  let (_res, _) = stream.write_all(info).await;
  while let Some(msg) = rx.recv().await {
    // println!("out {:?} to {:?}", msg, stream.peer_addr());
    let (res, _) = stream.write_all(msg).await;
    if res.is_err() {
      println!("{:} pipe_out error {:?}", id, res);
      break;
    }
  }
  println!("{:} pipe_out exiting", id);
  drop(rx);
  drop(stream);
}
