use anyhow::{Context, Ok};
use dist_sys::*;
use serde::{Deserialize, Serialize};
use std::{io::{StdoutLock}, sync::{Arc, Mutex}};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: Mutex<usize>,
}

impl Node<(), Payload> for EchoNode {
    async fn from_init(
        _state: (),
        _init: Init,
        _tx: tokio::sync::mpsc::UnboundedSender<Event<Payload>>,
        _output: &mut StdoutLock<'_>
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { 
            id: Mutex::new(1),
        })
    }

    async fn step(&self, input: Event<Payload>, output: Arc<Mutex<std::io::Stdout>>) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("no event injection");
        };

        let mut reply = {
            let mut id = self.id.lock().unwrap();
            input.into_reply(Some(&mut *id))
        };
        
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                reply.send(output).context("reply to echo")?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _, _, _>(()).await
}
