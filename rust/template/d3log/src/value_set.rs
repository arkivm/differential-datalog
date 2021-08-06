// a module to support a set of updates and provide some convencience functions
// over them, in particular general serde support. This currently sits on top
// of DeltaMap, but that might change

use crate::{Batch, Error, EvaluatorTrait, FactSet};
use differential_datalog::{ddval::DDValue, program::RelId, DeltaMap};
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

// exporting fields just we can have an external serializer
pub struct BatchInternal {
    pub timestamp: u64,
    pub deltas: DeltaMap<differential_datalog::ddval::DDValue>,
}

#[derive(Clone)]
pub struct ValueSet(pub Arc<Mutex<BatchInternal>>);

impl Display for ValueSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&"<")?;
        let batch_inner = self.0.lock().unwrap();
        for (relid, vees) in batch_inner.deltas.clone() {
            // I would really prefer a readable name..but we need an Evaluator and it doesn't seem right
            // to add it to all the batches or globalize it
            f.write_str(&format!("({}", relid))?;

            let mut m = 0;
            for (_v, _) in vees {
                m += 1;
            }
            f.write_str(&format!(" {})", m))?;
        }
        f.write_str(&">")?;
        Ok(())
    }
}

pub struct BatchIterator<'a> {
    relid: RelId,
    relations: Box<dyn Iterator<Item = (RelId, BTreeMap<DDValue, isize>)> + Send + 'a>,
    items: Option<Box<dyn Iterator<Item = (DDValue, isize)> + Send>>,
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = (RelId, DDValue, isize);

    fn next(&mut self) -> Option<(RelId, DDValue, isize)> {
        match &mut self.items {
            Some(x) => match x.next() {
                Some((v, w)) => Some((self.relid, v, w)),
                None => {
                    self.items = None;
                    self.next()
                }
            },
            None => {
                // what about the empty batch?
                let (relid, items) = self.relations.next()?;
                self.relid = relid;
                self.items = Some(Box::new(items.into_iter()));
                self.next()
            }
        }
    }
}

impl<'a> IntoIterator for &'a ValueSet {
    type Item = (RelId, DDValue, isize);
    type IntoIter = BatchIterator<'a>;

    fn into_iter(self) -> BatchIterator<'a> {
        BatchIterator {
            relid: 0,
            relations: Box::new(self.clone().0.lock().unwrap().deltas.clone().into_iter()),
            items: None,
        }
    }
}

// clippy says
impl Default for ValueSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueSet {
    pub fn from_delta_map(deltas: DeltaMap<differential_datalog::ddval::DDValue>) -> Batch {
        let n = Arc::new(Mutex::new(BatchInternal {
            deltas,
            timestamp: 0,
        }));
        Batch::new(FactSet::Empty(), FactSet::Value(ValueSet(n)))
    }

    pub fn new() -> ValueSet {
        ValueSet(Arc::new(Mutex::new(BatchInternal {
            deltas: DeltaMap::<differential_datalog::ddval::DDValue>::new(),
            timestamp: 0,
        })))
    }

    // should this return batch to allow for chaining? is that a thing?
    pub fn insert(&mut self, r: RelId, v: differential_datalog::ddval::DDValue, weight: isize) {
        self.0.lock().unwrap().deltas.update(r, &v, weight);
    }

    // this got recordified(?)
    //     pub fn singleton(rel: &str, v: &differential_datalog::ddval::DDValue) -> Result<Batch, Error> {
    //        let mrel = match Relations::try_from(rel) {
    //            Ok(x) => x as usize,
    //            Err(_x) => return Err(Error::new(format!("bad relation {}", rel))),
    //        };

    //        let mut b = Batch::new();
    //        b.insert(mrel, v.clone(), 1);
    //        Ok(b)
    //    }

    pub fn from(e: &dyn EvaluatorTrait, f: FactSet) -> Result<ValueSet, Error> {
        match f {
            FactSet::Value(x) => Ok(x),
            FactSet::Record(rb) => {
                let mut ddval_batch = ValueSet::new();
                let e2 = e.clone();
                for (r, v, w) in &rb {
                    let rid = e2.id_from_relation_name(r.clone())?;
                    ddval_batch.insert(rid, e.ddvalue_from_record(r, v)?, w);
                }
                Ok(ddval_batch)
            }
            FactSet::Empty() => Ok(ValueSet::new()),
        }
    }
}
