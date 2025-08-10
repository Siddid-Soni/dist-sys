use std::io::StdoutLock;
use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename="dest")]
    pub dst: String,
    pub body: Body<Payload>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename="msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload
}

pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}


pub trait Node<Payload> {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, Payload>(mut state: S) -> anyhow::Result<()>
where
    S:Node<Payload>,
    Payload: DeserializeOwned
{
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();
    
    let mut stdout = std::io::stdout().lock();

    for input in inputs {
        let input = input.context("Could not Deserialize Input")?;
        state
            .step(input, &mut stdout)
            .context("node step failed")?;
    }
    Ok(())
}
