use super::Entry;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, Duration};

use anyhow::{Result, anyhow};

use async_broadcast;
use async_channel;

use papaya::HashMap as PapayaMap;

enum Channel {
  Topic {
    sender: async_broadcast::Sender<Entry>,
    receiver: async_broadcast::InactiveReceiver<Entry>,
  },
  Queue {
    sender: async_channel::Sender<Entry>,
    receiver: async_channel::Receiver<Entry>,
  },
}

fn create(name: &str) -> Channel {
  if name.starts_with("q/") {
    let (sender, receiver) = async_channel::bounded::<Entry>(100);
    return Channel::Queue { sender, receiver };
  }

  let (mut sender, receiver) = async_broadcast::broadcast::<Entry>(100);
  sender.set_overflow(true);
  return Channel::Topic { sender, receiver: receiver.deactivate() }
}

pub enum Subscription {
  Topic(async_broadcast::Receiver<Entry>),
  Queue(async_channel::Receiver<Entry>),
}

impl Subscription {
  pub async fn recv(&mut self) -> Result<Entry> {
    match self {
      Self::Topic(receiver) => {
        receiver.recv().await
          .map_err(|err| anyhow!("{:?}", err))
      }
      Self::Queue(receiver) => {
        receiver.recv().await
          .map_err(|err| {
            println!("sub recv error {:?}", err);
            anyhow!("{:?}", err)
          })
      }
    }
  }

  pub fn is_closed(&self) -> bool {
    match self {
      Self::Topic(receiver) => { receiver.is_closed() }
      Self::Queue(receiver) => { receiver.is_closed() }
    }
  }

  pub fn close(&self) -> bool {
    match self {
      Self::Topic(receiver) => { receiver.close() }
      Self::Queue(receiver) => { receiver.close() }
    }
  }
}

pub enum Publisher {
  Topic(async_broadcast::Sender<Entry>),
  Queue(async_channel::Sender<Entry>),
}

impl Publisher {
  pub async fn send(&self, entry: Entry) -> Result<()> {
    match self {
      Self::Topic(sender) => {
        match sender.broadcast(entry).await {
          Ok(_) => { Ok(()) }
          Err(err) => { Err(anyhow!("{:?}", err)) }
        }
      }
      Self::Queue(sender) => {
        match sender.send(entry).await {
          Ok(_) => { Ok(()) }
          Err(err) => {
            println!("pub send error {:?}", err);
            Err(anyhow!("{:?}", err)) }
        }
      }
    }
  }

  pub fn is_closed(&self) -> bool {
    match self {
      Self::Topic(sender) => { sender.is_closed() }
      Self::Queue(sender) => { sender.is_closed() }
    }
  }

  pub fn close(&self) -> bool {
    match self {
      Self::Topic(sender) => { sender.close() }
      Self::Queue(sender) => { sender.close() }
    }
  }
}

impl Channel {
  fn subscribe(&self) -> Subscription {
    match self {
      Self::Topic { sender: _, receiver } => {
        Subscription::Topic(receiver.activate_cloned())
      }
      Self::Queue { sender: _, receiver } => {
        Subscription::Queue(receiver.clone())
      }
    }
  }

  fn publisher(&self) -> Publisher {
    match self {
      Self::Topic { sender, receiver: _ } => {
        Publisher::Topic(sender.clone())
      }
      Self::Queue { sender, receiver: _ } => {
        Publisher::Queue(sender.clone())
      }
    }
  }

  fn active(&self) -> bool {
    match self {
      Self::Topic { sender, receiver: _ } => {
        sender.sender_count() > 1 || sender.receiver_count() > 0
      }
      Self::Queue { sender, receiver: _ } => {
        sender.sender_count() > 1 && sender.receiver_count() > 1
      }
    }
  }

  fn close(&self) {
    match self {
      Self::Topic { sender, receiver } => {
        sender.close();
        receiver.close();
      }
      Self::Queue { sender, receiver } => {
        sender.close();
        receiver.close();
      }
    }
  }
}

pub struct Channels {
  map: PapayaMap<String, Channel>,
}

unsafe impl Sync for Channels {}

impl Channels {
  pub fn new() -> Self {
    Self { map: PapayaMap::new() }
  }

  pub fn subscribe(&self, name: &str) -> Subscription {
    return self.map.pin()
      .get_or_insert_with(name.to_string(), || create(name))
      .subscribe();
  }

  pub fn publisher(&self, name: &str) -> Publisher {
    return self.map.pin()
      .get_or_insert_with(name.to_string(), || create(&name))
      .publisher();
  }

  pub fn prune(&self) {
    let g = self.map.guard();
    for (name, channel) in self.map.iter(&g) {
      if !channel.active() {
        channel.close();
        self.map.remove(name, &g);
      }
    }
  }
}

// each thread has one shared publishers
pub struct Publishers {
  channels: Arc<Channels>,
  active: HashMap<String, (Publisher, Instant)>
}

impl Publishers {
  pub fn new(channels: Arc<Channels>) -> Self {
    Self { channels, active: HashMap::new() }
  }

  pub fn channel(&mut self, name: &str) -> &Publisher {
    return &self.active.entry(name.to_string())
      .and_modify(|(_, instant)| *instant = Instant::now())
      .or_insert_with(||
        (self.channels.publisher(name), Instant::now())
      ).0;
  }

  pub fn prune(&mut self) -> bool {
    const EXPIRE: Duration = Duration::from_secs(60);
    let initial_count = self.active.len();
    self.active.retain(|_, (publisher, last_used)| {
      if last_used.elapsed() < EXPIRE { return true; }
      publisher.close();
      return false;
    });
    return self.active.len() < initial_count;
  }
}
