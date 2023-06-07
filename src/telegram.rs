

use std::{fmt::Display, time::SystemTime};

use serde::{ Serialize, Deserialize };

use crate::topic::Topic;


#[derive(Debug, Serialize, Deserialize)]
pub struct Telegram {
    t: SystemTime,
    value: String
}

impl Telegram {

    pub fn build(v: String) -> Self {
        Self {
            t: SystemTime::now(),
            value: v
        }
    }

    pub fn handled_at(&self) -> u128 {
        self.t.elapsed().unwrap().as_nanos()
    }

}