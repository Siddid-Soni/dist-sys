use anyhow::Context;
use dist_sys::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock, sync::{Arc, Mutex}};
use tokio::sync::{oneshot, Mutex as AsyncMutex};

// Counter operations (from clients to counter)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
}

// KV operations (counter to/from seq-kv)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KvPayload {
    Read { key: String },
    ReadOk { value: usize },
    Cas { key: String, from: usize, to: usize },
    CasOk,
    Write { key: String, value: usize },
    WriteOk,
    Error { code: u32, text: String },
}

// Shared state that needs synchronization
#[derive(Debug)]
struct NodeState {
    id: usize,
    pending_kv_responses: HashMap<usize, oneshot::Sender<Result<usize, String>>>,
}

struct CounterNode {
    node: String,
    node_ids: Vec<String>,
    state: Mutex<NodeState>,
    add_lock: AsyncMutex<()>,
}

impl CounterNode {
    async fn kv_read(&self, key: String, output: Arc<Mutex<std::io::Stdout>>) -> anyhow::Result<(usize, oneshot::Receiver<Result<usize, String>>)> {
        let (msg_id, rx) = {
            let mut state = self.state.lock().unwrap();
            let msg_id = state.id;
            state.id += 1;

            let (tx, rx) = oneshot::channel();
            state.pending_kv_responses.insert(msg_id, tx);
            (msg_id, rx)
        }; // Lock is released here

        let msg = Message {
            src: self.node.clone(),
            dst: "seq-kv".to_string(),
            body: Body {
                id: Some(msg_id),
                in_reply_to: None,
                payload: KvPayload::Read { key: key.clone() },
            },
        };

        msg.send(output)
            .with_context(|| format!("failed to send read request for key {}", key))?;

        Ok((msg_id, rx))
    }

    async fn kv_cas(&self, key: String, from: usize, to: usize, output: Arc<Mutex<std::io::Stdout>>) -> anyhow::Result<(usize, oneshot::Receiver<Result<usize, String>>)> {
        let (msg_id, rx) = {
            let mut state = self.state.lock().unwrap();
            let msg_id = state.id;
            state.id += 1;

            let (tx, rx) = oneshot::channel();
            state.pending_kv_responses.insert(msg_id, tx);
            (msg_id, rx)
        }; // Lock is released here

        let msg = Message {
            src: self.node.clone(),
            dst: "seq-kv".to_string(),
            body: Body {
                id: Some(msg_id),
                in_reply_to: None,
                payload: KvPayload::Cas { key: key.clone(), from, to },
            },
        };

        msg.send(output)
            .with_context(|| format!("failed to send CAS request for key {} (from {} to {})", key, from, to))?;

        Ok((msg_id, rx))
    }

    fn kv_write(
        &self,
        key: String,
        value: usize,
        output: &mut StdoutLock<'_>,
    ) -> anyhow::Result<()> {
        let msg_id = {
            let mut state = self.state.lock().unwrap();
            let msg_id = state.id;
            state.id += 1;
            msg_id
        }; // Lock is released here

        let msg = Message {
            src: self.node.clone(),
            dst: "seq-kv".to_string(),
            body: Body {
                id: Some(msg_id),
                in_reply_to: None,
                payload: KvPayload::Write { key: key.clone(), value },
            },
        };
        msg.send_sync(output)
            .with_context(|| format!("failed to send write request for key {} with value {}", key, value))?;
        Ok(())
    }
}

impl Node<(), Payload, KvPayload> for CounterNode {
    async fn from_init(
        _state: (),
        init: Init,
        _tx: tokio::sync::mpsc::UnboundedSender<Event<Payload, KvPayload>>,
        output: &mut StdoutLock<'_>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = CounterNode {
            node_ids: init.node_ids,
            node: init.node_id.clone(),
            state: Mutex::new(NodeState {
                id: 0,
                pending_kv_responses: HashMap::new(),
            }),
            add_lock: AsyncMutex::new(()),
        };

        node.kv_write(init.node_id.clone(), 0, output)
            .with_context(|| format!("failed to initialize counter for node {}", init.node_id))?;
        Ok(node)
    }

    async fn step(
        &self,
        input: Event<Payload, KvPayload>,
        output: Arc<Mutex<std::io::Stdout>>,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                match input.body.payload {
                    Payload::Add { delta } => {
                        // Acquire the add lock to serialize Add operations
                        let _add_guard = self.add_lock.lock().await;
                        
                        loop {
                            let old_val = match self.kv_read(self.node.clone(), output.clone()).await {
                                Ok((_msg_id, rx)) => {
                                    match rx.await {
                                        Ok(Ok(value)) => value,
                                        Ok(Err(_)) => {
                                            // Key should exist
                                            continue;
                                        }
                                        Err(_) => continue, // Channel error
                                    }
                                }
                                Err(_) => continue, // Send error
                            };

                            match self.kv_cas(self.node.clone(), old_val, old_val + delta, output.clone()).await {
                                Ok((_msg_id, rx)) => {
                                    match rx.await {
                                        Ok(Ok(_)) => {
                                            // CAS succeeded
                                            break;
                                        }
                                        Ok(Err(_)) => {
                                            // CAS failed
                                            continue;
                                        }
                                        Err(_) => continue, // Channel error, retry
                                    }
                                }
                                Err(_) => continue, // Send error, retry
                            }
                        }

                        // Drop the add lock before sending the reply
                        drop(_add_guard);
                        
                        let mut reply = {
                            let mut state = self.state.lock().unwrap();
                            input.into_reply(Some(&mut state.id))
                        }; // Lock is released here
                        reply.body.payload = Payload::AddOk;
                        reply.send(output).context("failed to send Add response")?;
                    }

                    Payload::Read => {
                        // Set up all KV read requests and collect receivers
                        let mut receivers = Vec::new();
                        
                        for node_id in &self.node_ids.clone() {
                            match self.kv_read(node_id.clone(), output.clone()).await {
                                Ok((_msg_id, rx)) => receivers.push((node_id.clone(), rx)),
                                Err(e) => {
                                    eprintln!("Failed to send read request to node {}: {}", node_id, e);
                                }
                            }
                        }
                        
                        // wait for all responses (lock is not held)
                        let mut total_value = 0;
                        for (node_id, rx) in receivers {
                            match rx.await {
                                Ok(Ok(value)) => {
                                    total_value += value;
                                }
                                Ok(Err(e)) => {
                                    eprintln!("KV read error from node {}: {}", node_id, e);
                                }
                                Err(_) => {
                                    eprintln!("Failed to receive KV response from node {}", node_id);
                                }
                            }
                        }
                        
                        let mut reply = {
                            let mut state = self.state.lock().unwrap();
                            input.into_reply(Some(&mut state.id))
                        }; // Lock is released here

                        reply.body.payload = Payload::ReadOk { value: total_value };
                        reply.send(output).context("failed to send Read response")?;
                    }

                    Payload::AddOk | Payload::ReadOk { .. } => {
                        // Response messages, ignore
                    }
                }
            }

            Event::ServiceMessage(service_msg) => {
                match service_msg.body.payload {
                    KvPayload::ReadOk { value } => {
                        if let Some(msg_id) = service_msg.body.in_reply_to {
                            let tx_opt = {
                                let mut state = self.state.lock().unwrap();
                                state.pending_kv_responses.remove(&msg_id)
                            }; // Lock is released here

                            if let Some(tx) = tx_opt {
                                let _ = tx.send(Ok(value));
                            } else {
                                eprintln!("ERROR: got a read ok from non pending msg")
                            }
                        }
                    }

                    KvPayload::CasOk => {
                        if let Some(msg_id) = service_msg.body.in_reply_to {
                            let tx_opt = {
                                let mut state = self.state.lock().unwrap();
                                state.pending_kv_responses.remove(&msg_id)
                            }; // Lock is released here

                            if let Some(tx) = tx_opt {
                                let _ = tx.send(Ok(0)); // CAS success, 
                            }
                        }
                    }

                    KvPayload::Error { code: _, text } => {
                        if let Some(msg_id) = service_msg.body.in_reply_to {
                            let tx_opt = {
                                let mut state = self.state.lock().unwrap();
                                state.pending_kv_responses.remove(&msg_id)
                            }; // Lock is released here

                            if let Some(tx) = tx_opt {
                                let _ = tx.send(Err(text));
                            }
                        }
                    }
                    KvPayload::Cas { .. } => {}
                    KvPayload::Write { .. } => {}
                    KvPayload::Read { .. } => {}
                    KvPayload::WriteOk => {}
                }
            }

            Event::EOF => {}
            Event::Injected(_) => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<_, CounterNode, Payload, KvPayload, _>(()).await
}
