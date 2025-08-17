use anyhow::Context;
use dist_sys::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet}, io::StdoutLock, sync::{Arc, Mutex}, time::Duration
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

// Shared mutable state
#[derive(Debug)]
struct NodeState {
    id: usize,
    messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,
}

struct BroadcastNode {
    node: String,
    state: Mutex<NodeState>,
}

impl Node<(), Payload, (), InjectedPayload> for BroadcastNode {
    async fn from_init(
        _state: (),
        init: Init,
        tx: tokio::sync::mpsc::UnboundedSender<Event<Payload, (), InjectedPayload>>,
        _output: &mut StdoutLock<'_>
    ) -> anyhow::Result<Self> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if let Err(_) = tx.send(Event::Injected(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });

        Ok(Self {
            node: init.node_id.clone(),
            state: Mutex::new(NodeState {
                id: 1,
                messages: HashSet::new(),
                neighborhood: vec![],
                known: init
                    .node_ids
                    .into_iter()
                    .map(|nid| (nid, HashSet::new()))
                    .collect(),
            }),
        })
    }

    async fn step(
        &self,
        input: Event<Payload, (), InjectedPayload>,
        output: Arc<Mutex<std::io::Stdout>>,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::ServiceMessage(..) => {}
            
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    // Get current state snapshot
                    let (neighborhood, messages, known) = {
                        let state = self.state.lock().unwrap();
                        (state.neighborhood.clone(), state.messages.clone(), state.known.clone())
                    };

                    for n in &neighborhood {
                        let known_to_n = &known[n];
                        let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = messages
                            .iter()
                            .copied()
                            .partition(|m| known_to_n.contains(m));

                        let mut rng = rand::rng();
 
                        let additional_cap = (10 * notify_of.len() / 100) as u32;
                        notify_of.extend(already_known.iter().filter(|_| {
                            rng.random_ratio(
                                additional_cap.min(already_known.len() as u32),
                                already_known.len() as u32,
                            )
                        }));

                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen: notify_of },
                            },
                        }
                        .send(output.clone())
                        .with_context(|| format!("gossip to {}", n))?
                    }
                }
            },

            Event::Message(input) => {
                let mut reply = {
                    let mut state = self.state.lock().unwrap();
                    input.into_reply(Some(&mut state.id))
                };
                match reply.body.payload {
                    Payload::Gossip { seen } => {
                        let mut state = self.state.lock().unwrap();
                        state.known
                            .get_mut(&reply.dst)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());

                        state.messages.extend(seen);
                    }

                    Payload::Broadcast { message } => {
                        {
                            let mut state = self.state.lock().unwrap();
                            state.messages.insert(message);
                        } // Lock released here
                        
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(output).context("reply to broadcast")?;
                    }
                    Payload::Read => {
                        let messages = {
                            let state = self.state.lock().unwrap();
                            state.messages.clone()
                        };

                        reply.body.payload = Payload::ReadOk { messages };
                        reply.send(output).context("reply to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        {
                            let mut state = self.state.lock().unwrap();
                            state.neighborhood = topology
                                .remove(&self.node)
                                .unwrap_or_else(|| panic!("no topology given for node {}", self.node));
                        } // Lock released here

                        reply.body.payload = Payload::TopologyOk;
                        reply.send(output).context("reply to topology")?;
                    }
                    Payload::ReadOk { .. } | Payload::BroadcastOk { .. } | Payload::TopologyOk => {}
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _ ,_>(()).await
}
