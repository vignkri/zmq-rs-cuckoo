//! Telegram
//!
//! definitions for the things done internally
//! in the system for operational purposes

use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::SystemTime};

/// enumerated message payload that contains the value of the
/// data being passed
#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Data {
        symbol: Arc<str>,
        high: f32,
        low: f32,
    },
    Kill,
}

impl Message {
    /// generate telegram from the message
    pub fn to_telegram(self) -> Telegram {
        Telegram::build(self)
    }
}

/// generates the message
#[derive(Debug, Serialize, Deserialize)]
pub struct Telegram {
    t: SystemTime,
    value: Message,
}

impl Telegram {
    /// build a new telegram with the message
    pub fn build(v: Message) -> Self {
        Self {
            t: SystemTime::now(),
            value: v,
        }
    }

    /// get the internal payload
    pub fn get_payload(&self) -> &Message {
        &self.value
    }

    /// get the duration of when the message was constructed and handled
    pub fn handled_at(&self) -> u128 {
        self.t.elapsed().unwrap().as_nanos()
    }

    /// generate byte representation of the data
    pub fn to_string(&self) -> String {
        let as_string = serde_json::to_string(&self).unwrap();
        as_string
    }
}
