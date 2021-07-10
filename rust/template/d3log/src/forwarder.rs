use crate::{
    async_error, fact, Batch, DDValueBatch, Error, Evaluator, Node, Port, RecordBatch, Transport,
};
use differential_datalog::record::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Forwarder {
    eval: Evaluator,
    management: Port,
    fib: Arc<Mutex<HashMap<Node, Port>>>,
}

impl Forwarder {
    pub fn new(eval: Evaluator, management: Port) -> Forwarder {
        // ok - we dont really want to start another hddlog here, but it helps
        // quite a bit in reducing the amount of sharing going on through TM.
        // ideally we could ask this question without access to the whole machine?
        // or share better with the other guy

        Forwarder {
            eval,
            management,
            fib: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, n: Node, p: Port) {
        self.fib.lock().expect("lock").insert(n, p);
    }
}

use std::ops::DerefMut;

impl Transport for Forwarder {
    fn send(&self, b: Batch) {
        let mut output = HashMap::<Node, Box<DDValueBatch>>::new();

        for (rel, v, weight) in &DDValueBatch::from(&(*self.eval), b).expect("iterator") {
            // xxx - through an api
            if let Some((loc_id, in_rel, inner_val)) = self.eval.localize(rel, v.clone()) {
                // not sure I agree with inner_val .. guess so?
                output
                    .entry(loc_id)
                    .or_insert_with(|| Box::new(DDValueBatch::new()))
                    .deref_mut()
                    .insert(in_rel, inner_val, weight);
            }
        }

        for (nid, b) in output.drain() {
            // there is a short version of this like expect?
            match self.fib.lock().expect("lock").get(&nid) {
                Some(x) => x.send(Batch::Value(b.deref().clone())),
                None => async_error!(
                    self.management.clone(),
                    Err(Error::new("missing nid".to_string()))
                ),
            }
        }
    }
}
