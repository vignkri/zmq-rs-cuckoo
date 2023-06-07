use std::fmt::Display;

use serde::{ Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Topic { 
    Core,
    Events,
}

impl Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let outstr = match self {
            Topic::Core => "Core",
            Topic::Events => "Events",
        };

        write!(f, "{}", outstr)
    }
}