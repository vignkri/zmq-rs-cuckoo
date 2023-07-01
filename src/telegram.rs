

use std::{fmt::Display, time::SystemTime};

use serde::{ Serialize, Deserialize };

use crate::topic::Topic;


#[derive(Clone)]
pub enum SystemMessage {
    Exit
}



#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Value,
    Kill
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Telegram {
    t: SystemTime,
    value: Message
}

impl Telegram {

    pub fn build(v: Message) -> Self {
        Self {
            t: SystemTime::now(),
            value: v
        }
    }

    pub fn get_payload(&self) -> &Message {
        &self.value
    }

    pub fn handled_at(&self) -> u128 {
        self.t.elapsed().unwrap().as_nanos()
    }

}