use dist_sys::*;
use anyhow::{Context, Ok};
use serde::{Deserialize, Serialize};
use std::{io::{StdoutLock, Write}};


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag="type")]
#[serde(rename_all="snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize
}

impl Node<(), Payload> for EchoNode {
    fn from_init(_state: (), _init: Init) -> anyhow::Result<Self>
        where Self: Sized {
            Ok(EchoNode { id: 1 })
    }
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("newline")?;
                self.id += 1;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_,EchoNode,_>(())
}