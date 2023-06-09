use crossbeam::channel;
use rand::Rng;
use topic::Topic;
use zmq::Context;

use crate::telegram::Telegram;

mod metrics;
mod telegram;
mod topic;

// location of the backend
const BACKEND: &'static str = "tcp://127.0.0.1:2400";

/// execute the client within the thread
pub fn run_client(client_id: i32, endpoint: &str) -> anyhow::Result<()> {
    // connect to the zeromq system and subscribe to the topics neededticker_msg
    let subscriber = Context::new().socket(zmq::SUB)?;
    subscriber.connect(endpoint)?;

    // Topic
    let topics = vec![
        topic::Topic::Core.to_string(),
        topic::Topic::Events.to_string(),
    ];
    for t in topics {
        subscriber.set_subscribe(t.as_bytes())?;
        tracing::info!("{} Subscribing to topic: {}", &client_id, &t);
    }

    // generate a metrics slot for tracking the processing capacity
    let mut metrics = metrics::Metrics::new();

    loop {
        if let Ok(content) = subscriber.recv_multipart(zmq::SNDMORE) {
            let data = content
                .iter()
                .map(|v| String::from_utf8(v.to_owned()).unwrap())
                .collect::<Vec<String>>();

            let payload = data.get(1).unwrap().to_owned();
            // msg processed size so that the metrics can be updated
            let processed_size = std::mem::size_of_val(&payload);
            let telegram = serde_json::from_str::<Telegram>(&payload)?;
            let time_since_publish = telegram.handled_at();

            // track the bytes processed in the system
            metrics.add_bytes(processed_size);

            match telegram.get_payload() {
                telegram::Message::Data { .. } => {
                    // todo: handle per symbol, high, low and construct history
                    metrics.increment();
                    metrics.add(time_since_publish);
                }
                telegram::Message::Kill => {
                    let average_msg_handling_time = metrics.average_secs();
                    tracing::info!("Disconnecting and exiting: {}", &client_id);
                    tracing::info!(
                        "client={} n_messages={} avg_time_sec={} bytes_processed={}",
                        &client_id,
                        &metrics.n_msgs,
                        &average_msg_handling_time,
                        &metrics.bytes_processed,
                    );
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

    // build a current-thread runtime
    let rt = tokio::runtime::Builder::new_current_thread()
        .thread_name("clients")
        .enable_all()
        .build()
        .unwrap();

    let _client_threads = std::thread::spawn(move || {
        tracing::info!("Starting client thread");
        let handle = rt.handle();

        let mut task_handles = vec![];
        for v in 0..2 {
            let task = handle.spawn_blocking(move || {
                tracing::debug!("Launching subscriber... {}", &v);
                let r = run_client(v, BACKEND);
                r
            });
            task_handles.push(task);
        }
    });

    let publisher = Context::new().socket(zmq::PUB).unwrap();
    publisher.bind(BACKEND).expect("unable to bind to socket");

    let value_interval = channel::tick(std::time::Duration::from_micros(10));

    let mut metrics = metrics::Metrics::new();

    let mut rng = rand::thread_rng();
    let instant = std::time::Instant::now();
    loop {
        let (msg, topic) = crossbeam::select! {
            recv(value_interval) -> ticker_msg => {

                // if the ticker failed, try to continue and try later
                if ticker_msg.is_err() {
                    continue;
                };

                // generate a message for a single symbol
                let msg = telegram::Message::Data {
                    symbol: String::from("AAPL").into(),
                    high: rng.gen::<f32>(),
                    low: rng.gen::<f32>(),
                }.to_telegram();

                // send the message forward
                (msg, Topic::Core)
            }
        };

        // send forward
        publisher
            .send(topic.to_string().as_bytes(), zmq::SNDMORE)
            .unwrap();
        publisher.send(msg.to_string().as_bytes(), 0).unwrap();
        metrics.increment();

        // if elpased is more than 2 minutes, kill the system
        if instant.elapsed() > std::time::Duration::from_secs(120) {
            let topic_bytes = Topic::Core.to_string();
            let msg = telegram::Message::Kill.to_telegram();

            publisher
                .send(topic_bytes.as_bytes(), zmq::SNDMORE)
                .unwrap();
            publisher.send(msg.to_string().as_bytes(), 0).unwrap();

            // allow 2 seconds to cleanup tasks;
            tracing::debug!("Waiting to cleanup");
            std::thread::sleep(std::time::Duration::from_secs(2));
            break;
        }
    }
}
