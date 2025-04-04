use monoio;
use monoio::net::{TcpListener, ListenerOpts};
use monoio::time;

mod client;
mod channels;
use channels::{Channels, Publishers};

type Entry = Vec<u8>;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::thread::yield_now;

fn next_id() -> usize {
  static COUNTER: AtomicUsize = AtomicUsize::new(1);
  return COUNTER.fetch_add(1, Ordering::Relaxed);
}

fn runtime() -> monoio::RuntimeBuilder<time::TimeDriver<monoio::LegacyDriver>> {
  monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
    .enable_timer()
}

fn main() {
  let cores = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
  println!("total cores = {}", cores);
  let channels = Arc::new(Channels::new());

  let threads: Vec<_> = (1 .. cores).map(|_i| {
    let tc = channels.clone();
    thread::spawn(|| {
      // runtime().build().expect("Failed building the Runtime")
      //   .block_on(async move { thread(tc).await; });
      monoio::start::<time::TimeDriver<monoio::LegacyDriver>, _>(async {
        thread(tc).await;
      });
    })
  }).collect();

  // runtime().build().expect("Failed building the Runtime")
  //   .block_on(thread(listener, channels));

  threads.into_iter().for_each(|t| {
    let _ = t.join();
  });
}

async fn prune(channels: Arc<Channels>, publishers: Rc<RefCell<Publishers>>) {
  let thread_id = thread::current().id();
  loop {
    let next_prune_at = time::Instant::now() + time::Duration::from_secs(10);
    time::sleep_until(next_prune_at).await;
    println!("pruning inactive publishers and channels on thread {:?}", thread_id);
    let prune_channels = publishers.borrow_mut().prune();
    if prune_channels {
      channels.prune();
    }
  }
}

async fn thread(channels: Arc<Channels>) {
  let thread_id = thread::current().id();
  let thread_count = next_id() as u64;
  let publishers = Rc::new(RefCell::new(Publishers::new(channels.clone())));
  println!("thread {:?} ({})", thread_id, thread_count);
  // thread::sleep(Duration::from_secs(1u64 * thread_count));

  // monoio::spawn(prune(channels.clone(), publishers.clone()));

  let listener = TcpListener::bind_with_config(
    "127.0.0.1:8115",
    &ListenerOpts::new().reuse_port(true).reuse_addr(true)
  ).unwrap();
  println!("listening");
  loop {
    let incoming = listener.accept().await;
    match incoming {
      Ok((stream, addr)) => {
          println!("accepted a connection from {} on {:?}", addr, thread_id);
          client::start_client(stream, addr, channels.clone(), publishers.clone());
      }
      Err(e) => {
          println!("accepted connection failed: {}", e);
          return;
      }
    }
    yield_now();
    // thread::sleep(Duration::from_secs(10));
    time::sleep(time::Duration::from_millis(10_000)).await;
    yield_now();
  }
}
