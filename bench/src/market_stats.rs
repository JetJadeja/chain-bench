use crate::tx_tracker::TxLifecycle;

pub struct MarketStats {
    pub steady: PhaseStats,
    pub ramp: PhaseStats,
    pub burst: BurstStats,
}

pub struct PhaseStats {
    pub count: usize,
    pub confirmed: usize,
    pub reverted: usize,
    pub dropped: usize,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

pub struct BurstStats {
    pub count: usize,
    pub confirmed: usize,
    pub reverted: usize,
    pub dropped: usize,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_latency_ms: f64,
    pub completion_time_ms: Option<i64>,
}

fn percentile(sorted: &[i64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)] as f64
}

fn phase_stats(records: &[&TxLifecycle]) -> PhaseStats {
    let confirmed = records.iter().filter(|r| r.status == "confirmed").count();
    let reverted = records.iter().filter(|r| r.status == "reverted").count();
    let dropped = records.iter().filter(|r| r.status == "dropped").count();

    let mut latencies: Vec<i64> = records.iter().filter_map(|r| r.latency_ms).collect();
    latencies.sort();

    PhaseStats {
        count: records.len(),
        confirmed,
        reverted,
        dropped,
        p50_ms: percentile(&latencies, 50.0),
        p95_ms: percentile(&latencies, 95.0),
        p99_ms: percentile(&latencies, 99.0),
    }
}

impl MarketStats {
    pub fn compute(records: &[TxLifecycle], burst_start_ms: Option<i64>) -> Self {
        let steady: Vec<&TxLifecycle> = records.iter().filter(|r| r.phase == "steady").collect();
        let ramp: Vec<&TxLifecycle> = records.iter().filter(|r| r.phase == "ramp").collect();
        let burst_records: Vec<&TxLifecycle> =
            records.iter().filter(|r| r.phase == "burst").collect();

        let burst = {
            let base = phase_stats(&burst_records);

            let mut latencies: Vec<i64> =
                burst_records.iter().filter_map(|r| r.latency_ms).collect();
            latencies.sort();

            let max_latency_ms = latencies.last().copied().unwrap_or(0) as f64;

            let completion_time_ms = match burst_start_ms {
                Some(start) => burst_records
                    .iter()
                    .filter_map(|r| r.t_included_ms)
                    .max()
                    .map(|last| last - start),
                None => None,
            };

            BurstStats {
                count: base.count,
                confirmed: base.confirmed,
                reverted: base.reverted,
                dropped: base.dropped,
                p50_ms: base.p50_ms,
                p95_ms: base.p95_ms,
                p99_ms: base.p99_ms,
                max_latency_ms,
                completion_time_ms,
            }
        };

        Self {
            steady: phase_stats(&steady),
            ramp: phase_stats(&ramp),
            burst,
        }
    }

    pub fn print(&self, num_operators: u32, total_submitted: u64, total_failed: u64) {
        println!();
        println!("═══ Market Simulation Results ({num_operators} operator{}) ═══",
            if num_operators == 1 { "" } else { "s" });
        println!("  Total submitted: {total_submitted}  |  Send failures: {total_failed}");
        println!();

        print_phase("Steady", &self.steady);
        print_phase("Ramp", &self.ramp);

        println!(
            "  Burst ({} tx):  completed in {}",
            self.burst.count,
            match self.burst.completion_time_ms {
                Some(ms) => format!("{:.1}s", ms as f64 / 1000.0),
                None => "N/A".into(),
            }
        );
        println!(
            "                 p50 {:.0}ms  p95 {:.0}ms  p99 {:.0}ms",
            self.burst.p50_ms, self.burst.p95_ms, self.burst.p99_ms
        );
        println!(
            "                 max single-tx: {:.0}ms",
            self.burst.max_latency_ms
        );
        println!(
            "                 {} confirmed  {} reverted  {} dropped",
            self.burst.confirmed, self.burst.reverted, self.burst.dropped
        );
        println!();

        if let Some(ms) = self.burst.completion_time_ms {
            let cutoff = ms as f64 / 1000.0;
            println!("  → Trade cutoff: T-{cutoff:.1}s (burst p99: {:.0}ms)", self.burst.p99_ms);
        }
        println!();
    }
}

fn print_phase(name: &str, stats: &PhaseStats) {
    if stats.count == 0 {
        println!("  {name}: (no txs)");
        return;
    }
    println!(
        "  {name} ({} tx):  p50 {:.0}ms  p95 {:.0}ms  p99 {:.0}ms  {} drops",
        stats.count, stats.p50_ms, stats.p95_ms, stats.p99_ms, stats.dropped
    );
}

pub async fn write_csv(
    records: &[TxLifecycle],
    path: &std::path::Path,
) -> eyre::Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    for r in records {
        wtr.serialize(r)?;
    }
    wtr.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_basic() {
        let data = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        assert_eq!(percentile(&data, 50.0), 60.0); // nearest-rank: idx 4.5 rounds to 5
        assert_eq!(percentile(&data, 99.0), 100.0);
        assert_eq!(percentile(&data, 0.0), 10.0);
    }

    #[test]
    fn percentile_empty() {
        let data: Vec<i64> = vec![];
        assert_eq!(percentile(&data, 50.0), 0.0);
    }

    #[test]
    fn percentile_single() {
        let data = vec![42];
        assert_eq!(percentile(&data, 50.0), 42.0);
        assert_eq!(percentile(&data, 99.0), 42.0);
    }
}
