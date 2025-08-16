use anyhow::Context;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{StdoutLock, Write};
use tokio::io::{AsyncBufReadExt, BufReader};

#[allow(async_fn_in_trait)]

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

#[allow(async_fn_in_trait)]
pub trait Node<S, Payload, InjectedPayload = ()> {
    async fn from_init(
        state: S,
        init: Init,
        inject: tokio::sync::mpsc::UnboundedSender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    async fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

pub async fn main_loop<S, N, P, IP>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    N: Node<S, P, IP>,
    IP: Send + 'static,
{
    let stdin = tokio::io::stdin();
    let mut stdin = BufReader::new(stdin).lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next_line()
            .await?
            .expect("no init msg"),
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

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let mut node: N =
        N::from_init(init_state, init, tx.clone()).await.context("node initialization failed")?;

    let jh = tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut stdin = BufReader::new(stdin).lines();
        while let Some(line) = stdin.next_line().await? {
            let input = serde_json::from_str(&line).context("input could not be deserialized")?;

            if let Err(_) = tx.send(Event::Message(input)) {
                return Ok::<_, anyhow::Error>(());
            };
        }
        let _ = tx.send(Event::EOF);
        Ok(())
    });

    while let Some(input) = rx.recv().await {
        node.step(input, &mut stdout).await.context("node step failed")?;
    }

    jh.await
        .context("stdin task panicked")?
        .context("stdin task err")?;

    Ok(())
}
