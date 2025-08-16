use anyhow::Context;
use dist_sys::*;
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

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
    id: usize,
}

impl Node<(), Payload> for UniqueNode {
    async fn from_init(
        _state: (),
        init: Init,
        _tx: tokio::sync::mpsc::UnboundedSender<Event<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueNode {
            node: init.node_id,
            id: 1,
        })
    }
    async fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock<'_>) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("no event injection");
        };

        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.id);
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
    main_loop::<_, UniqueNode, _, _>(()).await
}
