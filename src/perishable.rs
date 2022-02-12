use chrono::{DateTime, Duration, Utc};
use crossbeam_utils::atomic::AtomicCell;

// First member holds a perishable metric T that expires at the instant given by the second member.
pub struct Perishable<T>(T, AtomicCell<DateTime<Utc>>);

impl<T> Perishable<T> {
    pub fn new(t: T) -> Self {
        Perishable(t, AtomicCell::new(Utc::now()))
    }

    pub fn freshen(&self, valid_duration: Duration) -> &T {
        self.1.store(Utc::now() + valid_duration);
        &self.0
    }

    pub fn fresh(&self) -> Option<&T> {
        if self.1.load() >= Utc::now() {
            Some(&self.0)
        } else {
            None
        }
    }

    pub fn map<U, F>(&self, f: F) -> Option<U>
    where
        F: FnMut(&T) -> U,
    {
        self.fresh().map(f)
    }
}
