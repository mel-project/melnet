use std::time::Instant;

/// A reputation tracker that automatically takes care of time.
pub struct RepTracker {
    reputation: f64,
    last_update: Instant,
}

impl Default for RepTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RepTracker {
    /// Create a new reptracker.
    pub fn new() -> Self {
        Self {
            reputation: 0.0,
            last_update: Instant::now(),
        }
    }

    /// Updates reputation.
    pub fn delta(&mut self, rep: f64) {
        if self.reputation < 20.0 {
            self.reputation += rep;
        }
    }

    /// Calculate current reputation.
    pub fn get_reputation(&self) -> f64 {
        self.reputation / 2.0f64.powf(self.last_update.elapsed().as_secs_f64() / 3600.0)
    }
}
