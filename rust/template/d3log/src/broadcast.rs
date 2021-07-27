// a replication component to support broadcast metadata facts. this includes 'split horizon' as a temporary
// fix for simple pairwise loops. This will need an additional distributed coordination mechanism in
// order to maintain a consistent spanning tree (and a strategy for avoiding storms for temporariliy
// inconsistent topologies

use crate::{Batch, Error, Node, Port, Transport};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct Ingress {
    index: usize,
    broadcast: Arc<Broadcast>,
}

#[derive(Clone, Default)]
pub struct Broadcast {
    id: Node,
    count: Arc<AtomicUsize>,
    pub ports: Arc<Mutex<Vec<(Port, usize)>>>,
}

impl Broadcast {
    pub fn new(id: Node) -> Arc<Broadcast> {
        Arc::new(Broadcast {
            id,
            count: Arc::new(AtomicUsize::new(0)),
            ports: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

// this is kind of ridiculous. i have to define this trait because it _needs_ to take
// an Arc(alpha), but we .. cant extend arc, except we can add a trait.

pub trait PubSub {
    fn subscribe(self, p: Port) -> Port;
    // xxx - we shouldn't be referring to the narrow object type Broadcast here,
    // but its not clear where else to wire this
    fn couple(self, b: Arc<Broadcast>) -> Result<(), Error>;
}

struct Trace {
    uuid: Node,
    head: String,
    p: Port,
}

impl Trace {
    fn new(uuid: Node, head: String, p: Port) -> Port {
        Arc::new(Trace {
            uuid,
            head,
            p: p.clone(),
        })
    }
}

impl Transport for Trace {
    fn send(&self, b: Batch) {
        println!("{} {} {} ", self.uuid, self.head, b);
        self.p.clone().send(b);
    }
}

impl PubSub for Arc<Broadcast> {
    fn subscribe(self, p: Port) -> Port {
        let index = self.count.fetch_add(1, Ordering::Acquire);
        let mut ports = self.ports.lock().expect("lock ports");
        ports.push((p, index));
        Arc::new(Ingress {
            broadcast: self.clone(),
            index,
        })
    }

    fn couple(self, b: Arc<Broadcast>) -> Result<(), Error> {
        let index = self.count.fetch_add(1, Ordering::Acquire);
        let p2 = b.subscribe(Arc::new(Ingress {
            broadcast: self.clone(),
            index,
        }));
        self.ports.lock().expect("lock").push((p2, index));
        return Ok(());
    }
}

impl Ingress {
    pub fn remove(&mut self, _p: Port) {}
}

impl Transport for Ingress {
    fn send(&self, b: Batch) {
        let ports = { &*self.broadcast.ports.lock().expect("lock").clone() };
        for (port, index) in ports {
            if *index != self.index {
                port.send(b.clone())
            }
        }
    }
}

impl Transport for Broadcast {
    fn send(&self, b: Batch) {
        // We clone this map to have a read-only copy, else, we'd open up the possiblity of a
        // deadlock, if this `send` forms a cycle.
        let ports = { &*self.ports.lock().expect("lock").clone() };
        for (port, _) in ports {
            port.send(b.clone())
        }
    }
}
