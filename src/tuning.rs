pub fn adjust_tuning(queue_len: usize, base_batch: usize, base_interval: u64) -> (usize, u64) {
    if queue_len > 100_000 {
        (base_batch * 4, base_interval / 2)
    } else if queue_len > 10_000 {
        (base_batch * 2, base_interval / 2)
    } else if queue_len > 1000 {
        (base_batch, base_interval)
    } else {
        (base_batch / 2, base_interval * 2)
    }
}
