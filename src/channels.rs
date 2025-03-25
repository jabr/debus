use super::Entry;

use anyhow::{Result, anyhow};

use async_broadcast;
use async_channel;

use papaya::HashMap;

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

enum Subscription {
  Topic(async_broadcast::Receiver<Entry>),
  Queue(async_channel::Receiver<Entry>),
}

impl Subscription {
  async fn recv(&mut self) -> Result<Entry> {
    match self {
      Self::Topic(receiver) => {
        receiver.recv().await
          .map_err(|err| anyhow!("{:?}", err))
      }
      Self::Queue(receiver) => {
        receiver.recv().await
          .map_err(|err| anyhow!("{:?}", err))
      }
    }
  }

  fn close(&mut self) -> bool {
    match self {
      Self::Topic(receiver) => { receiver.close() }
      Self::Queue(receiver) => { receiver.close() }
    }
  }
}

enum Publisher {
  Topic(async_broadcast::Sender<Entry>),
  Queue(async_channel::Sender<Entry>),
}

impl Publisher {
  async fn send(&self, entry: Entry) -> Result<()> {
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
          Err(err) => { Err(anyhow!("{:?}", err)) }
        }
      }
    }
  }

  fn close(&self) -> bool {
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
  map: HashMap<String, Channel>,
}

impl Channels {
  fn new() -> Self {
    Self { map: HashMap::new() }
  }

  fn subscribe(&self, name: &str) -> Subscription {
    return self.map.pin()
      .get_or_insert_with(name.to_string(), || create(name))
      .subscribe();
  }

  fn publisher(&self, name: &str) -> Publisher {
    return self.map.pin()
      .get_or_insert_with(name.to_string(), || create(&name))
      .publisher();
  }

  fn prune(&self) {
    let g = self.map.guard();
    for (name, channel) in self.map.iter(&g) {
      if !channel.active() {
        channel.close();
        self.map.remove(name, &g);
      }
    }
  }
}
