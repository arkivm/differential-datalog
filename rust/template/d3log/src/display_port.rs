use crate::{
    async_error, fact, function, send_error, Batch, Evaluator, Forwarder, Port, RecordBatch,
    Transport,
};
use differential_datalog::record::{IntoRecord, Record};
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use hyper_websocket_lite::{server_upgrade, AsyncClient};
use rand::prelude::*;
use std::borrow::Cow;
use std::convert::Infallible;
use std::str::from_utf8;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use websocket_codec::{Message, Opcode}; //MessageCodec

#[derive(Clone)]
pub struct DisplayPort {
    management: Port,
    data: Port,
    forwarder: Arc<Forwarder>,
    eval: Evaluator,
}

#[derive(Clone)]
pub struct Browser {
    eval: Evaluator,
    uuid: u128,
    sender: tokio::sync::mpsc::Sender<Message>,
}

impl Transport for Browser {
    fn send(self: &Self, b: Batch) {
        let self_clone = self.clone();
        let rb = RecordBatch::from(self.eval.clone(), b);
        // async error
        let encoded = serde_json::to_string(&rb).expect("send json encoding error");
        tokio::spawn(async move { self_clone.sender.send(Message::text(encoded)).await });
    }
}

const JS_BATCH_HANDLER: &[u8] = include_bytes!("display.js");

async fn on_client(disp_port: DisplayPort, stream_mut: AsyncClient) {
    let (tx, mut rx) = channel(100);

    let browser = Browser {
        eval: disp_port.eval.clone(),
        uuid: random::<u128>(),
        sender: tx.clone(),
    };
    disp_port
        .forwarder
        .register(browser.uuid, Arc::new(browser.clone()));

    disp_port.management.send(fact!(
        display::Browser,
        t => disp_port.eval.clone().now().into_record(),
        uuid => browser.uuid.into_record()));

    let (mut stx, srx) = stream_mut.split();

    let disp_clone = disp_port.clone();
    tokio::spawn(async move {
        loop {
            if let Some(msg) = rx.recv().await {
                async_error!(disp_clone.eval.clone(), stx.send(msg).await);
            }
        }
    });

    let mut crx = srx;
    let eval_clone = disp_port.eval.clone();
    loop {
        let (msg, next) = crx.into_future().await;

        match msg {
            Some(Ok(msg)) => match msg.opcode() {
                Opcode::Text | Opcode::Binary => {
                    // async error
                    let v = &msg.data().to_vec();
                    let s = from_utf8(v).expect("display json utf8 error");
                    println!("browser data {}", s.clone());
                    let v: RecordBatch =
                        serde_json::from_str(s.clone()).expect("display json parse error");
                    disp_port.data.clone().send(Batch::Rec(v));
                }
                Opcode::Ping => {
                    async_error!(
                        disp_port.eval.clone(),
                        tx.send(Message::pong(msg.into_data())).await
                    );
                }
                Opcode::Close => {}
                Opcode::Pong => {}
            },
            Some(Err(_err)) => {
                async_error!(eval_clone, tx.send(Message::close(None)).await);
            }
            None => {}
        }
        crx = next;
    }
}

async fn hello(disp_port: DisplayPort, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    if req.headers().contains_key("upgrade") {
        return server_upgrade(req, move |x| on_client(disp_port.clone(), x))
            .await
            .map_err(|_| panic!("upgrade"));
    }
    Ok(Response::new(Body::from(JS_BATCH_HANDLER)))
}

impl DisplayPort {
    pub async fn new(
        port: u16,
        eval: Evaluator,
        management: Port,
        forwarder: Arc<Forwarder>,
        data: Port,
    ) {
        let disp_port = DisplayPort {
            eval,
            management: management.clone(),
            data: data.clone(),
            forwarder: forwarder.clone(),
        };
        let disp_clone = disp_port.clone();
        // Open a listener socket at this port!
        let addr = ([0, 0, 0, 0], port).into();

        // Bind to localhost:port
        if let Ok(server) = Server::try_bind(&addr) {
            let server =
                server.serve(make_service_fn(move |_conn| {
                    let disp_port = disp_port.clone();
                    async move {
                        Ok::<_, Infallible>(service_fn(move |req| hello(disp_port.clone(), req)))
                    }
                }));
            println!("Listening on http://{}", addr);
            async_error!(disp_clone.eval.clone(), server.await);
        }
    }
}
