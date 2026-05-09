use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub enum Phase {
    Warmup,
    Ramp,
    Hold,
    Spike,
    Recovery,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Phase::Warmup => write!(f, "warmup"),
            Phase::Ramp => write!(f, "ramp"),
            Phase::Hold => write!(f, "hold"),
            Phase::Spike => write!(f, "spike"),
            Phase::Recovery => write!(f, "recovery"),
        }
    }
}

struct Segment {
    phase: Phase,
    start: Duration,
    end: Duration,
    tps: f64,
}

pub struct RampSchedule {
    segments: Vec<Segment>,
    start: Instant,
}

impl RampSchedule {
    pub fn new(
        target_rate: f64,
        warmup_secs: u64,
        ramp_step_secs: u64,
        ramp_multiplier: f64,
        duration_secs: u64,
        spike_multiplier: f64,
        spike_duration_secs: u64,
        recovery_secs: u64,
    ) -> Self {
        let mut segments = Vec::new();
        let mut offset = Duration::ZERO;
        let warmup_rate = target_rate * 0.1;

        // Warmup
        let warmup_dur = Duration::from_secs(warmup_secs);
        segments.push(Segment {
            phase: Phase::Warmup,
            start: offset,
            end: offset + warmup_dur,
            tps: warmup_rate,
        });
        offset += warmup_dur;

        // Ramp steps
        let mut current_rate = warmup_rate;
        while current_rate < target_rate {
            current_rate = (current_rate * ramp_multiplier).min(target_rate);
            let step_dur = Duration::from_secs(ramp_step_secs);
            segments.push(Segment {
                phase: Phase::Ramp,
                start: offset,
                end: offset + step_dur,
                tps: current_rate,
            });
            offset += step_dur;
        }

        // Hold: fill remaining time minus spike + recovery
        let tail = Duration::from_secs(spike_duration_secs + recovery_secs);
        let total = Duration::from_secs(duration_secs);
        let hold_dur = if total > offset + tail {
            total - offset - tail
        } else {
            Duration::from_secs(10)
        };
        segments.push(Segment {
            phase: Phase::Hold,
            start: offset,
            end: offset + hold_dur,
            tps: target_rate,
        });
        offset += hold_dur;

        // Spike
        let spike_dur = Duration::from_secs(spike_duration_secs);
        segments.push(Segment {
            phase: Phase::Spike,
            start: offset,
            end: offset + spike_dur,
            tps: target_rate * spike_multiplier,
        });
        offset += spike_dur;

        // Recovery
        let recovery_dur = Duration::from_secs(recovery_secs);
        segments.push(Segment {
            phase: Phase::Recovery,
            start: offset,
            end: offset + recovery_dur,
            tps: warmup_rate,
        });

        Self {
            segments,
            start: Instant::now(),
        }
    }

    pub fn current(&self) -> Option<(Phase, f64)> {
        let elapsed = self.start.elapsed();
        self.segments
            .iter()
            .find(|s| elapsed >= s.start && elapsed < s.end)
            .map(|s| (s.phase, s.tps))
    }

    pub fn inter_tx_delay(&self) -> Option<Duration> {
        self.current()
            .map(|(_, tps)| Duration::from_secs_f64(1.0 / tps))
    }

    pub fn total_duration(&self) -> Duration {
        self.segments.last().map(|s| s.end).unwrap_or_default()
    }
}
