
use std::ops::Index;

use topic::Topic;
use zmq::Context;

use crate::telegram::Telegram;

mod telegram;
mod topic;


const XPUB_PROXY: &'static str = "inproc://cuckoo_pub_proxy";
const XSUB_PROXY: &'static str = "inproc://cuckoo_sub_proxy";
// const BUS_ADDRS: &'static str = "tcp://127.0.0.1:2400";


fn run_publisher(topic: Topic, endpoint: &str) -> anyhow::Result<()> {

    let publisher = Context::new().socket(zmq::XPUB).unwrap();
    publisher.bind(endpoint).expect("unable to bind to socket");

    while true {
        let topic_bytes = topic.to_string();
        let payload = Telegram::build("Message".into());
        let stringified = serde_json::to_string(&payload)?;
        publisher.send(topic_bytes.as_bytes(), zmq::SNDMORE)?;
        publisher.send(stringified.as_bytes(), 0)?;

        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    Ok(())
}


fn run_client(client_id: i32, endpoint: &str) -> anyhow::Result<()> {

    let subscriber = Context::new().socket(zmq::XSUB)?;
    subscriber.connect(endpoint)?;

    // Topic
    let topics = vec![topic::Topic::Core.to_string(), topic::Topic::Events.to_string()];
    for t in topics {
        subscriber.set_subscribe(t.as_bytes())?;
        println!("{} Subscribing to topic: {}", &client_id, &t);
    }

    // subscriber.set_rcvtimeo(60)?;

    let mut msg_counter = 0;

    while true {

        let msg = subscriber.recv_multipart(zmq::SNDMORE);
        if msg.is_err() {
            println!("Couldn't receive: {:?}", msg);
            continue;
        }

        let content = msg.unwrap();
        let data = content.iter()
            .map(|v| String::from_utf8(v.to_owned()).unwrap())
            .collect::<Vec<String>>();

        let payload = data.get(1).unwrap().to_owned();
        let telegram = serde_json::from_str::<Telegram>(&payload)?;
        let time_since_publish = telegram.handled_at();
        msg_counter += 1;
        println!("{} ({}@{}ns) - Received bytes... {:?}", &client_id, &msg_counter, &time_since_publish, &data);
        
    }

    subscriber.disconnect(endpoint)?;
    println!("Disconnecting and exiting...");

    Ok(())
}


fn main() {

    // let socket = Context::new().socket(zmq::XPUB).expect("unable to create a proxy");
    // socket.bind(endpoint)


    // Send a topic and then follow up with a message
    for v in 0..5 {
        let client = std::thread::spawn(move || {
            println!("Launching subscribers...");
            run_client(v, XSUB_PROXY).unwrap();
        });

    }

    let publisher_topics = vec![Topic::Core, Topic::Events];
    for pt in publisher_topics {
        std::thread::spawn(move || {
            println!("Launching publisher with topic: {}", &pt);
            run_publisher(pt, XPUB_PROXY).unwrap();
        });
    }

    while true {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }

}
