use prometheus::{Gauge, Opts, Registry};

use crate::decoder;

pub struct WindMetrics {
    speed_magnitude: Gauge,
    source_direction: Gauge,
    component_velocity_north: Gauge,
    component_velocity_east: Gauge,
}

impl WindMetrics {
    pub fn new(name: &str, descr: &str) -> Self {
        let station = |name, help| {
            Opts::new(name, help)
                .namespace("tempest")
                .subsystem("station")
        };
        Self {
            speed_magnitude: Gauge::with_opts(station(
                format!("{}_speed_magnitude_m_per_s", name),
                format!("{} speed magnitude (m·s^-1)", descr),
            ))
            .unwrap(),
            source_direction: Gauge::with_opts(station(
                format!("{}_source_direction_deg", name),
                format!("{} source direction (deg)", descr),
            ))
            .unwrap(),
            component_velocity_north: Gauge::with_opts(station(
                format!("{}_component_velocity_north_m_per_s", name),
                format!("{} component velocity North (m·s^-1)", descr),
            ))
            .unwrap(),
            component_velocity_east: Gauge::with_opts(station(
                format!("{}_component_velocity_east_m_per_s", name),
                format!("{} component velocity East (m·s^-1)", descr),
            ))
            .unwrap(),
        }
    }

    pub fn register_all(&self, registry: &mut Registry) {
        registry
            .register(Box::new(self.speed_magnitude.clone()))
            .unwrap();
        registry
            .register(Box::new(self.source_direction.clone()))
            .unwrap();
        registry
            .register(Box::new(self.component_velocity_north.clone()))
            .unwrap();
        registry
            .register(Box::new(self.component_velocity_east.clone()))
            .unwrap();
    }

    pub fn export(&self, wind: &decoder::Wind) {
        self.speed_magnitude.set(wind.speed_magnitude());
        self.source_direction.set(wind.source_direction());
        let (north, east) = wind.component_velocity();
        self.component_velocity_north.set(north);
        self.component_velocity_east.set(east);
    }
}
