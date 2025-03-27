use super::{Entry, channels::{Channels, Subscription}};

use core::net::SocketAddr;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
  static COUNTER: AtomicUsize = AtomicUsize::new(1);
  return COUNTER.fetch_add(1, Ordering::Relaxed);
}

struct Client {
  channels: Channels,
  id: usize,
  addr: SocketAddr,
  stream: BufReader<OwnedReadHalf<TcpStream>>,
  out: Tx<Entry>,
  subscriptions: HashMap<String, Rc<RefCell<Subscription>>>,
}

impl Client {
  fn subscribe(&mut self, name: &str) {
    let subscription = Rc::new(RefCell::new(self.channels.subscribe(name)));
    self.subscriptions.insert(name.to_string(), subscription.clone());
    monoio::spawn(relay(subscription.clone(), self.out.clone()));
  }

  fn unsubscribe(&mut self, name: &str) {
    if let Some(subscription) = self.subscriptions.get(name) {
      subscription.borrow().close();
    }
  }

  fn publish(&self, name: &str, data: &Entry) {
    // threadlocal lookup for publisher
  }

  async fn serve(&mut self) {
    loop {
      // read line
      let mut buffer = Vec::new();
      if let Ok(count) = self.stream.read_until(b'\n', &mut buffer).await {
        if count == 0 { break; }
        println!("in {} {:?} from {:?}", count, buffer, self.addr);

        // broadcast
        // let res = publisher.send(buffer).await;
        // if res.is_err() {
        //   println!("problem broadcasting");
        // }
      } else { break; }
    }
  }
}

pub fn start_client(
  stream: TcpStream,
  addr: SocketAddr,
  channels: Channels
) {
  let (stream_read, stream_write) = stream.into_split();
  let (channel_tx, channel_rx) = channel::<Entry>(100);

  monoio::spawn(pipe_out(channel_rx, stream_write));
  monoio::spawn(async move {
    let mut client = Client {
      id: next_id(),
      addr,
      stream: BufReader::new(stream_read),
      out: channel_tx,
      subscriptions: HashMap::new(),
      channels,
    };

    client.serve().await;
  });
}

async fn relay(
  from: Rc<RefCell<Subscription>>,
  to: Tx<Entry>
) -> () {
  // @todo: notify on error?
  while let Ok(msg) = from.borrow_mut().recv().await {
    let _res = to.send(msg).await;
    // @todo: check res for error other than full channel
    if to.is_closed() { break; }
  }
  from.borrow().close();
  drop(to);
}

async fn pipe_out(
  mut rx: Rx<Entry>,
  mut stream: OwnedWriteHalf<TcpStream>
) -> () {
  while let Some(msg) = rx.recv().await {
    println!("out {:?} to {:?}", msg, stream.peer_addr());
    let (res, _) = stream.write_all(msg).await;
    if res.is_err() { break; }
  }
  rx.close();
  drop(rx);
  drop(stream);
}
