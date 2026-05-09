use serde::Serialize;
use std::path::Path;
use tokio::sync::mpsc;

#[derive(Serialize, Clone)]
pub struct TxRecord {
    pub nonce: u64,
    pub submit_timestamp_ms: i64,
    pub tx_hash: String,
    pub confirm_timestamp_ms: Option<i64>,
    pub block_number: Option<u64>,
    pub gas_used: Option<u64>,
    pub effective_gas_price: Option<u128>,
    pub status: Option<bool>,
    pub latency_ms: Option<i64>,
    pub phase: String,
}

pub struct Summary {
    pub total_submitted: u64,
    pub total_included: u64,
    pub total_reverted: u64,
    pub total_dropped: u64,
    pub offered_tps: f64,
    pub included_tps: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub avg_gas_per_match: f64,
    pub avg_effective_gas_price: f64,
    pub drop_rate: f64,
}

impl Summary {
    pub fn compute(records: &[TxRecord], duration_secs: f64) -> Self {
        let total_submitted = records.len() as u64;
        let included: Vec<&TxRecord> = records.iter().filter(|r| r.status == Some(true)).collect();
        let reverted: Vec<&TxRecord> = records.iter().filter(|r| r.status == Some(false)).collect();
        let dropped: Vec<&TxRecord> = records.iter().filter(|r| r.status.is_none()).collect();

        let total_included = included.len() as u64;
        let total_reverted = reverted.len() as u64;
        let total_dropped = dropped.len() as u64;

        let mut latencies: Vec<i64> = records.iter().filter_map(|r| r.latency_ms).collect();
        latencies.sort();

        let percentile = |p: f64| -> f64 {
            if latencies.is_empty() {
                return 0.0;
            }
            let idx = ((p / 100.0) * (latencies.len() as f64 - 1.0)).round() as usize;
            latencies[idx.min(latencies.len() - 1)] as f64
        };

        let avg_gas: f64 = if included.is_empty() {
            0.0
        } else {
            included.iter().filter_map(|r| r.gas_used).sum::<u64>() as f64
                / included.len() as f64
        };

        let avg_price: f64 = if included.is_empty() {
            0.0
        } else {
            included
                .iter()
                .filter_map(|r| r.effective_gas_price)
                .sum::<u128>() as f64
                / included.len() as f64
        };

        Self {
            total_submitted,
            total_included,
            total_reverted,
            total_dropped,
            offered_tps: total_submitted as f64 / duration_secs,
            included_tps: total_included as f64 / duration_secs,
            latency_p50_ms: percentile(50.0),
            latency_p95_ms: percentile(95.0),
            latency_p99_ms: percentile(99.0),
            avg_gas_per_match: avg_gas,
            avg_effective_gas_price: avg_price,
            drop_rate: if total_submitted == 0 {
                0.0
            } else {
                total_dropped as f64 / total_submitted as f64
            },
        }
    }

    pub fn print(&self) {
        println!("\n=== Benchmark Results ===");
        println!("Submitted:       {}", self.total_submitted);
        println!("Included:        {}", self.total_included);
        println!("Reverted:        {}", self.total_reverted);
        println!("Dropped:         {}", self.total_dropped);
        println!("Offered TPS:     {:.2}", self.offered_tps);
        println!("Included TPS:    {:.2}", self.included_tps);
        println!("Latency p50:     {:.0} ms", self.latency_p50_ms);
        println!("Latency p95:     {:.0} ms", self.latency_p95_ms);
        println!("Latency p99:     {:.0} ms", self.latency_p99_ms);
        println!("Avg gas/match:   {:.0}", self.avg_gas_per_match);
        println!("Avg gas price:   {:.0} wei", self.avg_effective_gas_price);
        println!("Drop rate:       {:.2}%", self.drop_rate * 100.0);
    }
}

pub async fn csv_writer_task(
    mut rx: mpsc::Receiver<TxRecord>,
    path: &Path,
) -> eyre::Result<Vec<TxRecord>> {
    let mut wtr = csv::Writer::from_path(path)?;
    let mut all_records = Vec::new();

    while let Some(record) = rx.recv().await {
        wtr.serialize(&record)?;
        wtr.flush()?;
        all_records.push(record);
    }

    Ok(all_records)
}
