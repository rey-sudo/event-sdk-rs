use uuid::Uuid;
use pulsar::{DeserializeMessage, Payload};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct EventEnveloped {
    pub event_id: Uuid,
    pub event_type: String,
    pub entity_type: String,
    pub data: serde_json::Value,
}

/// Implementation of the `DeserializeMessage` trait to transform Pulsar messages into `EventEnveloped`.
impl DeserializeMessage for EventEnveloped {
    type Output = Result<EventEnveloped, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum SubscriptionType {
    Shared,
    KeyShared,
}

impl SubscriptionType {
    pub fn parse(t: &str) -> Self {
        match t {
            "shared" => SubscriptionType::Shared,
            "key-shared" => SubscriptionType::KeyShared,
            _ => panic!("Invalid topic type: {}", t),
        }
    }
}