// functions for managing forked children.
// - start_node(), which is the general d3log runtime start, and probably doesn't belong here
// - implementation of Port over pipes
//
// this needs to get broken apart or shifted a little since the children in the future
// will be other ddlog executables

// this is all quite rendundant with the various existing wrappers, especially tokio::process
// however, we need to be inside the child fork before exec to add the management
// descriptors, otherwise most of this could go

use crate::{
    async_error,
    error::Error,
    fact,
    json_framer::JsonFramer,
    record_batch::{record_deserialize_batch, record_serialize_batch, RecordBatch},
    Batch, Evaluator, Port, Transport,
};
use differential_datalog::record::*;
use nix::unistd::*;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, convert::TryFrom, ffi::CString};
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, spawn};
use tokio_fd::AsyncFd;

type Fd = std::os::unix::io::RawFd;

// these could be allocated dynamically and passed in env
pub const MANAGEMENT_INPUT_FD: Fd = 3;
pub const MANAGEMENT_OUTPUT_FD: Fd = 4;

// xxx - this should follow trait Network
// really we probably want to have a forwarding table
// xxx child read input

#[derive(Clone)]
pub struct FileDescriptorPort {
    pub fd: Fd,
    //    eval: Evaluator,
    pub management: Port,
}

impl Transport for FileDescriptorPort {
    fn send(&self, b: Batch) {
        let js = async_error!(self.management, record_serialize_batch(b));
        let mut pin = AsyncFd::try_from(self.fd).expect("asynch");
        spawn(async move { pin.write_all(&js).await });
    }
}

async fn read_output<F>(fd: Fd, mut callback: F) -> Result<(), Error>
where
    F: FnMut(&[u8]),
{
    let mut pin = AsyncFd::try_from(fd)?;
    let mut buffer = [0; 1024];
    loop {
        let res = pin.read(&mut buffer).await?;
        callback(&buffer[0..res]);
    }
}

#[derive(Clone)]
pub struct ProcessManager {
    eval: Evaluator,
    processes: Arc<Mutex<HashMap<Pid, Arc<Mutex<Child>>>>>,
    management: Port,
    //    tm: ArcTransactionManager, // maybe we just need a port here
}

impl Transport for ProcessManager {
    fn send(&self, b: Batch) {
        // we think the dispatcher has given only facts from our relation
        // this should be from, is that not so?
        for (_, p, weight) in &RecordBatch::from(self.eval.clone(), b) {
            // what about other values of weight?
            if weight == -1 {
                // kill if we can find the uuid..i guess and if the total weight is 1
            }
            if weight == 1 {
                self.make_child(self.eval.clone(), p, self.management.clone())
                    .expect("fork failure");
                //                self.processes.lock().expect("lock").insert(v.id, pid);
            }
        }
    }
}

struct Child {
    uuid: u128,
    eval: Evaluator,
    //pid: Pid,
    management: Port,
    //    management_to_child: Port, // xxx - hook up to broadcast
}

impl Child {
    pub fn report_status(&self) {
        self.management.send(fact!(
            d3_application::ProcessStatus,
            id => self.uuid.into_record(),
            memory_bytes => 0.into_record(),
            threads => 0.into_record(),
            time => self.eval.now().into_record()));
    }
}

impl ProcessManager {
    pub fn new(eval: Evaluator, management: Port) -> ProcessManager {
        // allocate wait thread
        ProcessManager {
            eval,
            processes: Arc::new(Mutex::new(HashMap::new())),
            management,
        }
    }

    // arrange to listen to management channels if they exist
    // this should manage a filesystem resident cache of executable images,
    // potnetially addressed with a uuid or a url

    // in the earlier model, we deliberately refrained from
    // starting the multithreaded tokio runtime until after
    // we'd forked all the children. lets see if we can
    // can fork this executable w/o running exec if tokio has been
    // started.

    // since this is really an async error maybe deliver it here
    pub fn make_child(
        &self,
        eval: Evaluator,
        process: Record,
        management: Port,
    ) -> Result<(), Error> {
        // ideally we wouldn't allocate the management pair
        // unless we were actually going to use it..

        let (management_in_r, _management_in_w) = pipe().unwrap();
        let (management_out_r, management_out_w) = pipe().unwrap();

        let (standard_in_r, _standard_in_w) = pipe().unwrap();
        let (standard_out_r, standard_out_w) = pipe().unwrap();
        let (standard_err_r, standard_err_w) = pipe().unwrap();

        let id = process.get_struct_field("id").unwrap();

        match unsafe { fork() } {
            Ok(ForkResult::Parent { child }) => {
                // move above so we dont have to try to undo the fork on error
                let child_obj = Arc::new(Mutex::new(Child {
                    eval,
                    uuid: u128::from_record(id)?,
                    //pid: child,
                    management: management.clone(),
                    // management_to_child: Arc::new(FileDescriptorPort {
                    //                        management: management.clone(),
                    //                        fd: management_in_w,
                    //                    }),
                }));

                if process.get_struct_field("management").is_some() {
                    let child_clone = child_obj.clone();
                    spawn(async move {
                        let mut jf = JsonFramer::new();
                        read_output(management_out_r, move |b: &[u8]| {
                            (|| -> Result<(), Error> {
                                for i in jf.append(b)? {
                                    let v = record_deserialize_batch(i)?;
                                    // let v: RecordBatch = serde_json::from_str(&i)?;
                                    child_clone.clone().lock().expect("lock").management.send(v);
                                    // we shouldn't be doing this on every input - demo hack
                                    // i guess we just limit it to one
                                    child_clone.clone().lock().expect("lock").report_status();
                                }
                                Ok(())
                            })()
                            .expect("mangement forward");
                        })
                        .await
                        .expect("json read");
                    });
                }

                spawn(async move {
                    read_output(standard_out_r, |b: &[u8]| {
                        // utf8 framing issues?
                        print!("child {} {}", child, std::str::from_utf8(b).expect(""));
                        // assert
                    })
                    .await
                });

                spawn(async move {
                    read_output(standard_err_r, |b: &[u8]| {
                        println!("child error {}", std::str::from_utf8(b).expect(""));
                        // assert
                    })
                    .await
                });
                self.processes
                    .lock()
                    .expect("lock")
                    .insert(child, child_obj);
                Ok(())
            }

            Ok(ForkResult::Child) => {
                // plumb stdin and stdout regardless

                if process.get_struct_field("executable").is_some() {
                    dup2(management_out_w, MANAGEMENT_OUTPUT_FD)?;
                    dup2(management_in_r, MANAGEMENT_INPUT_FD)?;
                }

                dup2(standard_in_r, 0)?;
                dup2(standard_out_w, 1)?;
                dup2(standard_err_w, 2)?;

                //unsafe {
                if let Some(exec) = process.get_struct_field("executable") {
                    // FIXME: Temporary fix. this should be fixed ddlog-wide
                    let exec = exec.to_string().replace("\"", "");
                    if let Some(id) = process.get_struct_field("id") {
                        let path = CString::new(exec.clone()).expect("CString::new failed");
                        let arg0 = CString::new(exec).expect("CString::new failed");
                        // assign the child uuid here and pass in environment, just to avoid
                        // having to deal with getting it from the child asynchronously
                        // take a real map and figure out how to get a &[Cstring]
                        let u = format!("uuid={}", id);
                        let env1 = CString::new(u).expect("CString::new failed");

                        // ideally error would be wired up. execve returns Infallible
                        execve(&path, &[arg0], &[env1])?;
                    }
                }
                Ok(())
                // misformed process record?
            }
            Err(_e) => {
                panic!("Fork failed!");
            }
        }
    }
}
