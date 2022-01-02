mod wind_metrics;

use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};

use crate::decoder;
use crate::StationParams;
use wind_metrics::WindMetrics;

pub struct Exporter {
    metrics: ExportedMetrics,
    registry: Registry,
    station_params: StationParams,
}

impl Exporter {
    pub fn new(station_params: StationParams) -> Self {
        let metrics = ExportedMetrics::new();
        let mut registry = Registry::new();
        metrics.register_all(&mut registry);
        Self {
            metrics,
            registry,
            station_params,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        buffer
    }

    pub fn handle_report(&self, msg: &decoder::TempestMsg) {
        use decoder::TempestMsg as TM;
        match msg {
            TM::PrecipEvent(pe) => pe.export_to(&self.metrics, &self.station_params),
            TM::StrikeEvent(se) => se.export_to(&self.metrics, &self.station_params),
            TM::RapidWind(rw) => rw.export_to(&self.metrics, &self.station_params),
            TM::Observation(obs) => obs.export_to(&self.metrics, &self.station_params),
            TM::DeviceStatus(ds) => ds.export_to(&self.metrics, &self.station_params),
            TM::HubStatus(hs) => hs.export_to(&self.metrics, &self.station_params),
        }
    }
}

pub struct ExportedMetrics {
    exporter_messages_received: IntCounterVec,

    instant_wind: WindMetrics,

    observation_timestamp: IntGauge,
    observation_wind_lull: WindMetrics,
    observation_wind_avg: WindMetrics,
    observation_wind_gust: WindMetrics,
    observation_station_pressure: Gauge,
    observation_barometric_pressure: Gauge,
    observation_temperature: Gauge,
    observation_relative_humidity: Gauge,
    observation_dew_point: Gauge,
    observation_wet_bulb_temperature: Gauge,
    observation_apparent_temperature: Gauge,
    observation_illuminance: Gauge,
    observation_irradiance: Gauge,
    observation_uv_index: Gauge,
    observation_rain: Histogram,

    station_battery_volts: Gauge,
}

impl ExportedMetrics {
    fn new() -> Self {
        let station = |name, help| {
            Opts::new(name, help)
                .namespace("tempest")
                .subsystem("station")
        };
        let exporter = |name, help| {
            Opts::new(name, help)
                .namespace("tempest")
                .subsystem("exporter")
        };
        Self {
            exporter_messages_received: IntCounterVec::new(
                exporter("messages_received", "API messages received"),
                &["type"],
            )
            .unwrap(),

            instant_wind: WindMetrics::new("instant_wind", "Instantaneous wind"),

            observation_timestamp: IntGauge::with_opts(station(
                "observation_timestamp_unix_sec",
                "Current observation Unix timestamp (s)",
            ))
            .unwrap(),
            observation_wind_lull: WindMetrics::new("observation_wind_lull", "3-minute wind lull"),
            observation_wind_avg: WindMetrics::new("observation_wind_avg", "3-minute wind average"),
            observation_wind_gust: WindMetrics::new("observation_wind_gust", "3-minute wind gust"),
            observation_station_pressure: Gauge::with_opts(station(
                "observation_station_pressure_hpa",
                "Current station pressure (hPa)",
            ))
            .unwrap(),
            observation_barometric_pressure: Gauge::with_opts(station(
                "observation_barometric_pressure_hpa",
                "Current barometric pressure, mean sea level (hPa)",
            ))
            .unwrap(),
            observation_temperature: Gauge::with_opts(station(
                "observation_temperature_deg_c",
                "Current temperature (°C)",
            ))
            .unwrap(),
            observation_relative_humidity: Gauge::with_opts(station(
                "observation_relative_humidity_pct",
                "Current relative humidity (%)",
            ))
            .unwrap(),
            observation_dew_point: Gauge::with_opts(station(
                "observation_dew_point_deg_c",
                "Current dew point (°C)",
            ))
            .unwrap(),
            observation_wet_bulb_temperature: Gauge::with_opts(station(
                "observation_wet_bulb_temperature_deg_c",
                "Current wet bulb temperature (°C)",
            ))
            .unwrap(),
            observation_apparent_temperature: Gauge::with_opts(station(
                "observation_apparent_temperature_deg_c",
                "Current apparent temperature, Steadman formula (°C)",
            ))
            .unwrap(),
            observation_illuminance: Gauge::with_opts(station(
                "observation_illuminance_lux",
                "Current photometric illuminance (lux)",
            ))
            .unwrap(),
            observation_irradiance: Gauge::with_opts(station(
                "observation_irradiance_w_per_m2",
                "Current radiometric irradiance (W·m^-2)",
            ))
            .unwrap(),
            observation_uv_index: Gauge::with_opts(station(
                "observation_uv_index",
                "Current ultraviolet index",
            ))
            .unwrap(),
            observation_rain: Histogram::with_opts(
                HistogramOpts::from(station("observation_rain", "Rain observed (mm·min^-1)"))
                    .buckets(
                        prometheus::exponential_buckets(1.00, 10.0f64.powf(0.2), 17)
                            .unwrap()
                            .into_iter()
                            .map(|v| v.round() / 1000.0)
                            .collect(),
                    ),
            )
            .unwrap(),

            station_battery_volts: Gauge::with_opts(station(
                "status_battery_volts",
                "Station battery voltage (V)",
            ))
            .unwrap(),
        }
    }

    fn register_all(&self, registry: &mut Registry) {
        registry
            .register(Box::new(self.exporter_messages_received.clone()))
            .unwrap();

        self.instant_wind.register_all(registry);

        registry
            .register(Box::new(self.observation_timestamp.clone()))
            .unwrap();
        self.observation_wind_lull.register_all(registry);
        self.observation_wind_avg.register_all(registry);
        self.observation_wind_gust.register_all(registry);
        registry
            .register(Box::new(self.observation_station_pressure.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_barometric_pressure.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_temperature.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_relative_humidity.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_dew_point.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_wet_bulb_temperature.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_apparent_temperature.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_illuminance.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_irradiance.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_uv_index.clone()))
            .unwrap();
        registry
            .register(Box::new(self.observation_rain.clone()))
            .unwrap();

        registry
            .register(Box::new(self.station_battery_volts.clone()))
            .unwrap();
    }
}

trait ExportTo {
    fn export_to(&self, metrics: &ExportedMetrics, station_params: &StationParams);
}

impl ExportTo for decoder::PrecipEvent {
    fn export_to(&self, metrics: &ExportedMetrics, _station_params: &StationParams) {
        metrics
            .exporter_messages_received
            .with_label_values(&["precip_event"])
            .inc();
    }
}

impl ExportTo for decoder::StrikeEvent {
    fn export_to(&self, metrics: &ExportedMetrics, _station_params: &StationParams) {
        metrics
            .exporter_messages_received
            .with_label_values(&["strike_event"])
            .inc();
    }
}

impl ExportTo for decoder::RapidWind {
    fn export_to(&self, metrics: &ExportedMetrics, _station_params: &StationParams) {
        metrics
            .exporter_messages_received
            .with_label_values(&["instant_wind"])
            .inc();
        metrics.instant_wind.export(&self.wind);
    }
}

impl ExportTo for decoder::Observation {
    fn export_to(&self, metrics: &ExportedMetrics, station_params: &StationParams) {
        metrics
            .exporter_messages_received
            .with_label_values(&["observation"])
            .inc();
        metrics
            .observation_timestamp
            .set(self.timestamp.timestamp());
        metrics.observation_wind_lull.export(&self.wind_lull);
        metrics.observation_wind_avg.export(&self.wind_avg);
        metrics.observation_wind_gust.export(&self.wind_gust);
        metrics
            .observation_station_pressure
            .set(self.station_pressure);
        metrics
            .observation_barometric_pressure
            .set(self.barometric_pressure(station_params.elevation));
        metrics.observation_temperature.set(self.air_temperature);
        metrics
            .observation_relative_humidity
            .set(self.relative_humidity);
        metrics.observation_dew_point.set(self.dew_point());
        metrics
            .observation_wet_bulb_temperature
            .set(self.wet_bulb_temperature());
        metrics
            .observation_apparent_temperature
            .set(self.apparent_temperature());
        metrics.observation_illuminance.set(self.illuminance);
        metrics.observation_irradiance.set(self.irradiance);
        metrics.observation_uv_index.set(self.ultraviolet_index);
        metrics.observation_rain.observe(self.rain_last_minute);

        metrics.station_battery_volts.set(self.battery_volts);
    }
}

impl ExportTo for decoder::DeviceStatus {
    fn export_to(&self, metrics: &ExportedMetrics, _station_params: &StationParams) {
        metrics
            .exporter_messages_received
            .with_label_values(&["device_status"])
            .inc();
    }
}

impl ExportTo for decoder::HubStatus {
    fn export_to(&self, metrics: &ExportedMetrics, _station_params: &StationParams) {
        metrics
            .exporter_messages_received
            .with_label_values(&["hub_status"])
            .inc();
    }
}
