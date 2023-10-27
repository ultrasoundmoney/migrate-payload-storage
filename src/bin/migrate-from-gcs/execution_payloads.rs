use std::fmt;

use csv::ByteRecord;
use serde::{Deserialize, Serialize};

use crate::slots::Slot;

#[derive(Deserialize, Serialize)]
pub struct ExecutionPayload {
    block_hash: String,
    #[serde(skip_serializing)]
    id: String,
    inserted_at: String,
    payload: serde_json::Value,
    proposer_pubkey: String,
    slot: Slot,
    version: String,
}

impl ExecutionPayload {
    pub fn payload_block_hash(&self) -> &str {
        self.payload["block_hash"].as_str().unwrap()
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }
}

impl fmt::Debug for ExecutionPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload_block_hash = self.payload_block_hash();
        f.debug_struct("ExecutionPayload")
            .field("block_hash", &self.block_hash)
            .field("id", &self.id)
            .field("inserted_at", &self.inserted_at)
            .field("proposer_pubkey", &self.proposer_pubkey)
            .field("slot", &self.slot)
            .field("version", &self.version)
            .field(
                "payload",
                &format!("<PAYLOAD block_hash={}>", payload_block_hash),
            )
            .finish()
    }
}

impl From<ByteRecord> for ExecutionPayload {
    fn from(value: ByteRecord) -> Self {
        Self {
            block_hash: String::from_utf8(value[4].into()).unwrap(),
            id: String::from_utf8(value[0].into()).unwrap(),
            inserted_at: String::from_utf8(value[1].into()).unwrap(),
            payload: serde_json::from_slice(&value[6]).unwrap(),
            proposer_pubkey: String::from_utf8(value[3].into()).unwrap(),
            slot: std::str::from_utf8(&value[2])
                .unwrap()
                .parse()
                .map(Slot)
                .unwrap(),
            version: String::from_utf8(value[5].into()).unwrap(),
        }
    }
}
