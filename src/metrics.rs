/// store information on the number of messages being passed
/// and handled by the task
pub struct Metrics {
    pub bytes_processed: u128,
    pub n_msgs: u128,
    elapsed_time: Vec<u128>,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            bytes_processed: 0,
            n_msgs: 0,
            elapsed_time: vec![],
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            n_msgs: 0,
            bytes_processed: 0,
            elapsed_time: vec![],
        }
    }

    pub fn increment(&mut self) {
        self.n_msgs += 1;
    }

    pub fn reset_msg_counter(&mut self) {
        self.n_msgs = 0;
    }

    pub fn add_bytes(&mut self, size_of: usize) {
        self.bytes_processed += size_of as u128;
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
