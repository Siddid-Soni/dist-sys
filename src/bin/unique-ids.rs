use anyhow::Context;
use dist_sys::*;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueNode {
    node: String,
    id: Mutex<usize>,
}

impl Node<(), Payload> for UniqueNode {
    async fn from_init(
        _state: (),
        init: Init,
        _tx: tokio::sync::mpsc::UnboundedSender<Event<Payload>>,
        _output: &mut StdoutLock<'_>
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueNode {
            node: init.node_id,
            id: Mutex::new(1),
        })
    }
    async fn step(&self, input: Event<Payload>, output: Arc<Mutex<std::io::Stdout>>) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("no event injection");
        };

        let (mut reply, guid_id) = {
            let mut id = self.id.lock().unwrap();
            let guid_id = *id;  // Capture ID before into_reply increments it
            let reply = input.into_reply(Some(&mut *id));
            (reply, guid_id)
        };
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, guid_id);
                reply.body.payload = Payload::GenerateOk { guid };

                reply.send(output).context("reply to generate")?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _, _, _>(()).await
}
