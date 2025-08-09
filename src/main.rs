use std::io::{StdoutLock, Write};
use anyhow::{bail, Context};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Message {
    src: String,
    #[serde(rename="dest")]
    dst: String,
    body: Body
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Body {
    #[serde(rename="msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag="type")]
#[serde(rename_all="snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo : String },
    Init { node_id: String, node_ids: Vec<String> },
    InitOk
}

struct EchoNode {
    id: usize
}

impl EchoNode {
    pub fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { .. } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk
                    }
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("newline")?;
                self.id += 1;
            }

            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo }
                    }
                };
                serde_json::to_writer(&mut *output, &reply).context("serialize response")?;
                output.write_all(b"\n").context("newline")?;
                self.id += 1;
            }
            Payload::InitOk { .. } => bail!("recieved init_ok"),
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    
    let mut stdout = std::io::stdout().lock();

    let mut state= EchoNode { id: 0 };

    for input in inputs {
        let input = input.context("Could not Deserialize Input")?;
        state
            .step(input, &mut stdout)
            .context("node step failed")?;
    }
    Ok(())
}
