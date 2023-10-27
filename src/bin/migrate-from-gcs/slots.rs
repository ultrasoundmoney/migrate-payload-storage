use std::fmt;

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = "2020-12-01T12:00:23Z".parse().unwrap();
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Slot(pub i32);

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}", self.0)
    }
}

impl Slot {
    const SECONDS_PER_SLOT: u8 = 12;

    pub fn date_time(&self) -> DateTime<Utc> {
        self.into()
    }
}

impl From<&Slot> for DateTime<Utc> {
    fn from(slot: &Slot) -> Self {
        let seconds = slot.0 as i64 * Slot::SECONDS_PER_SLOT as i64;
        *GENESIS_TIMESTAMP + chrono::Duration::seconds(seconds)
    }
}
