use anyhow::Context;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{BufRead, StdoutLock, Write};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("serialize response")?;
        output.write_all(b"\n").context("newline")?;
        Ok(())
    }
}

pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    EOF,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Node<S, Payload, InjectedPayload = ()> {
    fn from_init(
        state: S,
        init: Init,
        inject: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

pub fn main_loop<S, N, P, IP>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    N: Node<S, P, IP>,
    IP: Send + 'static,
{
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("no init msg")
            .context("failed to read init msg")?,
    )
    .context("init could not be deserialised")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first msg shoulb be init");
    };

    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };
    serde_json::to_writer(&mut stdout, &reply).context("serialize response")?;
    stdout.write_all(b"\n").context("newline")?;

    drop(stdin);

    let (tx, rx) = std::sync::mpsc::channel();

    let mut node: N =
        Node::from_init(init_state, init, tx.clone()).context("node initialization failed")?;

    let jh = std::thread::spawn(move || {
        let stdin  = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.context("input could not be read")?;
            let input = serde_json::from_str(&line).context("input could not be deserialized")?;

            if let Err(_) = tx.send(Event::Message(input)) {
                return Ok::<_, anyhow::Error>(());
            };
        }
        let _ = tx.send(Event::EOF);
        Ok(())
    });

    for input in rx {
        node.step(input, &mut stdout).context("node step failed")?;
    }

    jh.join()
        .expect("thread panicked")
        .context("stdin thread err")?;

    Ok(())
}
