
use rand::Rng;
use telegram::SystemMessage;
use topic::Topic;
use zmq::Context;
use crossbeam::channel;

use crate::telegram::Telegram;

mod telegram;
mod topic;

const BACKEND: &'static str = "tcp://127.0.0.1:2400";

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

    pub fn reset_msg_counter(&mut self) {
        self.n_msgs = 0;
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
    let mut values: Vec<i32> = vec![];
    let mut moving_average: Option<f32> = None;

    loop {

        if let Ok(content) = subscriber.recv_multipart(zmq::SNDMORE) {
            let data = content.iter()
                .map(|v| String::from_utf8(v.to_owned()).unwrap())
                .collect::<Vec<String>>();

            let payload = data.get(1).unwrap().to_owned();
            let telegram = serde_json::from_str::<Telegram>(&payload)?;
            let time_since_publish = telegram.handled_at();

            match telegram.get_payload() {
                telegram::Message::Value (v) => {
                    values.push(*v);
                    if moving_average.is_none() {
                        moving_average = Some(*v as f32);
                    } else {
                        moving_average = Some((moving_average.unwrap() + *v as f32) / 2.0);
                    };

                    metrics.increment();
                    metrics.add(time_since_publish);
                },
                telegram::Message::ComputeValueMean => {
                    let n = values.len() as f32;
                    let v_f32 = values.drain(..).sum::<i32>() as f32;
                    let avg = v_f32 / n;
                    // tracing::info!("Generated average price: {}", avg);
                },
                telegram::Message::Kill => {
                    let average_msg_handling_time = metrics.average_secs();
                    tracing::info!("Disconnecting and exiting: {}", &client_id);
                    tracing::info!("client={} n_messages={} avg_time_sec={}", &client_id, &metrics.n_msgs, &average_msg_handling_time);
                    break;
                }
            };
        } else {
            tracing::error!("unable to recv");
        }
        
    }

    subscriber.disconnect(endpoint)?;
    tracing::debug!("Disconnecting and exiting...");

    Ok(())
}


fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    // Send a topic and then follow up with a message
    let rt = tokio::runtime::Builder::new_current_thread()
    .thread_name("clients")
    .enable_all()
    .build()
    .unwrap();

    let client_thread = std::thread::spawn(move || { 
        tracing::info!("Starting client thread");
        let handle = rt.handle();

        let mut task_handles = vec![];
        for v in 0..5 {
            let task = handle.spawn_blocking( move || {
                tracing::debug!("Launching subscriber... {}", &v);
                let r = run_client(v, BACKEND);
                r
            });
            task_handles.push(task);
        };
    });

    let publisher = Context::new().socket(zmq::PUB).unwrap();
    publisher.bind(BACKEND).expect("unable to bind to socket");

    let value_interval = channel::tick(std::time::Duration::from_micros(10));
    let avg_interval = channel::tick(std::time::Duration::from_secs(1));

    let mut metrics = Metrics::new();

    let mut rng = rand::thread_rng();
    let instant = std::time::Instant::now();
    loop {
        
        crossbeam::select! {
            recv(value_interval) -> _msg => {
                let topic_bytes = Topic::Core.to_string();
                let value_created = rng.gen();
                let payload = Telegram::build(telegram::Message::Value(value_created));
                let stringified = serde_json::to_string(&payload).unwrap();
                publisher.send(topic_bytes.as_bytes(), zmq::SNDMORE).unwrap();
                publisher.send(stringified.as_bytes(), 0).unwrap();
                metrics.increment();
            },
            recv(avg_interval) -> _msg => {
                let topic_bytes = Topic::Core.to_string();
                let payload = Telegram::build(telegram::Message::ComputeValueMean);
                let stringified = serde_json::to_string(&payload).unwrap();
                publisher.send(topic_bytes.as_bytes(), zmq::SNDMORE).unwrap();
                publisher.send(stringified.as_bytes(), 0).unwrap();
                metrics.increment();
            }
        };

        // if elpased is more than 2 minutes, kill the system
        if instant.elapsed() > std::time::Duration::from_secs(120) {
            let topic_bytes = Topic::Core.to_string();
            let payload = Telegram::build(telegram::Message::Kill);
            let stringified = serde_json::to_string(&payload).unwrap();
            publisher.send(topic_bytes.as_bytes(), zmq::SNDMORE).unwrap();
            publisher.send(stringified.as_bytes(), 0).unwrap();

            // allow 2 seconds to cleanup tasks;
            tracing::debug!("Waiting to cleanup");
            std::thread::sleep(std::time::Duration::from_secs(2));
            break;
        }
    }

}
