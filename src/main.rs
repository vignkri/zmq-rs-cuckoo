
use tokio::runtime::{Runtime, Handle};
use topic::Topic;
use zmq::Context;
use crossbeam::channel;

use crate::telegram::Telegram;

mod telegram;
mod topic;


const XPUB_PROXY: &'static str = "inproc://pub.proxy";
const XSUB_PROXY: &'static str = "inproc://sub.proxy";

pub struct Metrics {
    n_msgs: u128,
    elapsed_time: Vec<u128>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            n_msgs: 0,
            elapsed_time: vec![],
        }
    }

    pub fn increment(&mut self) {
        self.n_msgs += 1;
    }

    pub fn add(&mut self, et: u128) {
        self.elapsed_time.push(et);
    }

    pub fn average_nanoseconds(&self) -> f32 {
        let sum = self.elapsed_time.iter().sum::<u128>() as f32;
        sum / (self.n_msgs as f32)
    }

    pub fn average_secs(&self) -> f32 {
        let avgns = self.average_nanoseconds();
        avgns * 10e-9
    }
}


fn run_publisher(topic: Topic, endpoint: &str) -> anyhow::Result<()> {

    let publisher = Context::new().socket(zmq::PUB).unwrap();
    publisher.connect(endpoint).expect("unable to bind to socket");

    let message_interval = channel::tick(std::time::Duration::from_millis(1));
    let metrics_interval = channel::tick(std::time::Duration::from_secs(30));

    let mut metrics = Metrics::new();

    loop {

        crossbeam::select! {
            recv(message_interval) -> _msg => {
                let topic_bytes = topic.to_string();
                let payload = Telegram::build(telegram::Message::Value);
                let stringified = serde_json::to_string(&payload)?;
                publisher.send(topic_bytes.as_bytes(), zmq::SNDMORE)?;
                publisher.send(stringified.as_bytes(), 0)?;
                metrics.increment();
            },
            recv(metrics_interval) -> _msg => {
                let topic_bytes = topic.to_string();
                let payload = Telegram::build(telegram::Message::PrintMetrics);
                let stringified = serde_json::to_string(&payload)?;
                publisher.send(topic_bytes.as_bytes(), zmq::SNDMORE)?;
                publisher.send(stringified.as_bytes(), 0)?;
                tracing::info!("Published: {}", metrics.n_msgs);
            }
        };
    }

    Ok(())
}


pub fn run_client(client_id: i32, endpoint: &str) -> anyhow::Result<()> {

    let subscriber = Context::new().socket(zmq::SUB)?;
    subscriber.connect(endpoint)?;

    // Topic
    let topics = vec![topic::Topic::Core.to_string(), topic::Topic::Events.to_string()];
    for t in topics {
        subscriber.set_subscribe(t.as_bytes())?;
        tracing::info!("{} Subscribing to topic: {}", &client_id, &t);
    }

    // subscriber.set_rcvtimeo(60)?;
    let mut metrics = Metrics::new();

    loop {

        let msg = subscriber.recv_multipart(zmq::SNDMORE);
        if msg.is_err() {
            tracing::info!("Couldn't receive: {:?}", msg);
            break;
        }

        let content = msg.unwrap();
        let data = content.iter()
            .map(|v| String::from_utf8(v.to_owned()).unwrap())
            .collect::<Vec<String>>();

        let payload = data.get(1).unwrap().to_owned();
        let telegram = serde_json::from_str::<Telegram>(&payload)?;
        let time_since_publish = telegram.handled_at();

        match telegram.get_payload() {
            telegram::Message::Value => {
                metrics.increment();
                metrics.add(time_since_publish);
            }
            telegram::Message::PrintMetrics => {
                // print the metrics
                let average_time = metrics.average_nanoseconds();
                let average_time_secs = metrics.average_secs();
                tracing::info!("{}: {} [{} ns] -> {} s", &client_id, &metrics.n_msgs, &average_time, &average_time_secs);
            }
        };
        
    }

    subscriber.disconnect(endpoint)?;
    tracing::info!("Disconnecting and exiting...");

    Ok(())
}


fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    /// Publishes to the receivers
    let pub_proxy = Context::new().socket(zmq::XPUB).expect("Create proxy");
    pub_proxy.bind(XPUB_PROXY).unwrap();

    let sub_proxy = Context::new().socket(zmq::XSUB).expect("Create subscription proxy");
    sub_proxy.bind(XSUB_PROXY).unwrap();
    
    // Send a topic and then follow up with a message
    let rt = tokio::runtime::Builder::new_current_thread()
    .thread_name("clients")
    .enable_all()
    .build()
    .unwrap();


    let publisher_topics = vec![(Topic::Core, "inproc://core"), (Topic::Events, "inproc://events")];
    let mut publisher_handles = vec![];
    for (pt, _ept) in publisher_topics {
        let thh = std::thread::spawn(move || {
            tracing::info!("Launching publisher with topic: {}", &pt);
            run_publisher(pt, XSUB_PROXY).unwrap();
        });
        publisher_handles.push(thh);
    }

    let client_thread = std::thread::spawn(move || { 
        tracing::info!("Starting client thread");
        let handle = rt.handle();

        let mut task_handles = vec![];
        for v in 0..2 {
            let task = handle.spawn_blocking( move || {
                tracing::info!("Launching subscriber... {}", &v);
                let r = run_client(v, XPUB_PROXY);
                r
            });
            task_handles.push(task);
        };

        loop {
            // do nothing
            tracing::trace!("Heartbeat on the client thread");
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
    });

    let instant = std::time::Instant::now();
    loop {
        
        // 
        if instant.elapsed() > std::time::Duration::from_secs(120) {
            publisher_handles.iter().for_each(|v| drop(v));
            break;
        }

        // Send sigkill? 

        std::thread::sleep(std::time::Duration::from_secs(10));
    }

}
