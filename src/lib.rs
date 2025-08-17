use anyhow::Context;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{StdoutLock, Write};
use tokio::io::{AsyncBufReadExt, BufReader};
use std::sync::{Arc, Mutex};

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

    pub fn send_sync(&self, output: &mut StdoutLock<'_>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("serialize response")?;
        output.write_all(b"\n").context("newline")?;
        Ok(())
    }

    pub fn send(&self, output: Arc<Mutex<std::io::Stdout>>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let mut guard = output.lock().unwrap();
        let out = guard.by_ref();
        serde_json::to_writer(&mut *out, self).context("serialize response")?;
        out.write_all(b"\n").context("newline")?;
        drop(guard);
        Ok(())
    }
}

pub enum Event<Payload, ServicePayload = (), InjectedPayload = ()> {
    Message(Message<Payload>),
    ServiceMessage(Message<ServicePayload>),
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
pub trait Node<S, Payload, ServicePayload = (), InjectedPayload = ()>: Send + Sync {
    async fn from_init(
        state: S,
        init: Init,
        inject: tokio::sync::mpsc::UnboundedSender<Event<Payload, ServicePayload, InjectedPayload>>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &self,  // Changed from &mut self to &self
        input: Event<Payload, ServicePayload, InjectedPayload>,
        output: Arc<std::sync::Mutex<std::io::Stdout>>,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + std::marker::Send;
}

pub async fn main_loop<S, N, P, SP, IP>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    SP: DeserializeOwned + Send + 'static,
    N: Node<S, P, SP, IP> + Send + 'static,
    IP: Send + 'static,
{
    let stdin = tokio::io::stdin();
    let mut stdin = BufReader::new(stdin).lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> =
        serde_json::from_str(&stdin.next_line().await?.expect("no init msg"))
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

    let node: N = N::from_init(init_state, init, tx.clone(), &mut stdout)
        .await
        .context("node initialization failed")?;

    let jh = tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut stdin= BufReader::new(stdin).lines();
        while let Some(line) = stdin.next_line().await? {
            // Parse the JSON to extract src field for context
            let raw_value: serde_json::Value =
                serde_json::from_str(&line).context("input could not be parsed as JSON")?;

            let src = raw_value.get("src").and_then(|v| v.as_str()).unwrap_or("");

            // Check if source is not a client (clients typically start with 'c' like c1, c2, etc.)
            let is_client = (src.starts_with('c') || src.starts_with('n'))
                && src.chars().skip(1).all(|c| c.is_ascii_digit());

            // If not from a client, try service message first
            if is_client {
                if let Ok(node_msg) = serde_json::from_str::<Message<P>>(&line) {
                    if let Err(_) = tx.send(Event::Message(node_msg)) {
                        return Ok::<_, anyhow::Error>(());
                    };
                    continue;
                }
            }

            if let Ok(service_msg) = serde_json::from_str::<Message<SP>>(&line) {
                if let Err(_) = tx.send(Event::ServiceMessage(service_msg)) {
                    return Ok::<_, anyhow::Error>(());
                };
                continue;
            } else {
                eprintln!("Could not deserialize message from {}: {}", src, line);
            }
        }
        let _ = tx.send(Event::EOF);
        Ok(())
    });
    drop(stdout);

    let stdout = Arc::new(Mutex::new(std::io::stdout()));
    let node = Arc::new(node);
    while let Some(input) = rx.recv().await {
        let stdout_clone = stdout.clone();
        let node_clone = node.clone();
        tokio::spawn(async move {
            node_clone.step(input, stdout_clone).await.unwrap();
        });
    }

    jh.await
        .context("stdin task panicked")?
        .context("stdin task err")?;

    Ok(())
}
