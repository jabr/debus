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
  Null {
    sender: u8,
    receiver: u8,
  }
}

pub trait Receiver {
  async fn recv(&mut self) -> Result<Entry>;
}

impl Receiver for async_broadcast::Receiver<Entry> {
  async fn recv(&mut self) -> Result<Entry> {
    match self.recv().await {
      Ok(res) => { Ok(res) }
      Err(err) => { Err(anyhow!("{:?}", err)) }
    }
  }
}

impl Channel {
  // fn subscribe(&self) -> impl Receiver {
  fn subscribe(&self) -> async_broadcast::Receiver<Entry> {
    match &self {
      Channel::Topic { sender: _, receiver } => {
        receiver.activate_cloned()
      }
      _ => { panic!("not implemented") }
    }
  }

  async fn send(&self, entry: Entry) -> Result<()> {
    match &self {
      Channel::Topic { sender, receiver: _ } => {
        match sender.broadcast(entry).await {
          Ok(_) => { Ok(()) }
          Err(err) => { Err(anyhow!("{:?}", err)) }
        }
      }
      Channel::Queue { sender, receiver: _ } => {
        match sender.send(entry).await {
          Ok(_) => { Ok(()) }
          Err(err) => { Err(anyhow!("{:?}", err)) }
        }
      }
      Channel::Null { sender: _, receiver: _ }=> { Ok(()) }
    }
  }
}

fn subscribe_topic(map: &mut HashMap<String, Channel>, name: &str) -> async_broadcast::Receiver<Entry> {
  map.pin().get_or_insert_with(name.to_string(), || {
    let (mut sender, receiver) =
      async_broadcast::broadcast::<Entry>(100);
    sender.set_overflow(true);
    Channel::Topic {
      sender: sender.clone(),
      receiver: receiver.clone().deactivate(),
    }
  }).subscribe()
}
