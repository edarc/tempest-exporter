use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{anyhow, bail};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use futures_core::stream::Stream;
use log::{info, warn};
use tokio_stream::StreamExt;

use crate::reader::{self, RawTempestMsg};

#[derive(Debug)]
pub enum TempestMsg {
    PrecipEvent(PrecipEvent),
    StrikeEvent(StrikeEvent),
    RapidWind(RapidWind),
    Observation(Observation),
    DeviceStatus(DeviceStatus),
    HubStatus(HubStatus),
}

impl TryFrom<RawTempestMsg> for TempestMsg {
    type Error = (RawTempestMsg, anyhow::Error);
    fn try_from(msg: RawTempestMsg) -> Result<TempestMsg, Self::Error> {
        use RawTempestMsg as RM;
        use TempestMsg as TM;
        match msg {
            RM::PrecipEvent(rpe) => Ok(TM::PrecipEvent(rpe.into())),
            RM::StrikeEvent(rse) => Ok(TM::StrikeEvent(rse.into())),
            RM::RapidWind(rrw) => Ok(TM::RapidWind(rrw.into())),
            RM::Observation(ro) => ro
                .try_into()
                .map_err(|(ro, e)| (RM::Observation(ro), e))
                .map(TM::Observation),
            RM::HubStatus(rhs) => rhs
                .try_into()
                .map_err(|(rhs, e)| (RM::HubStatus(rhs), e))
                .map(TM::HubStatus),
            RM::DeviceStatus(rds) => Ok(TM::DeviceStatus(rds.into())),
        }
    }
}

#[derive(Debug)]
pub struct PrecipEvent {
    pub timestamp: DateTime<Utc>,
}

impl From<reader::RawPrecipEvent> for PrecipEvent {
    fn from(raw: reader::RawPrecipEvent) -> Self {
        Self {
            timestamp: DateTime::from_utc(NaiveDateTime::from_timestamp(raw.evt.0, 0), Utc),
        }
    }
}

#[derive(Debug)]
pub struct StrikeEvent {
    pub timestamp: DateTime<Utc>,
    pub distance: f64,
    pub energy: f64,
}

impl From<reader::RawStrikeEvent> for StrikeEvent {
    fn from(raw: reader::RawStrikeEvent) -> Self {
        Self {
            timestamp: DateTime::from_utc(NaiveDateTime::from_timestamp(raw.evt.0, 0), Utc),
            distance: raw.evt.1,
            energy: raw.evt.2,
        }
    }
}

#[derive(Debug)]
pub struct Wind {
    speed_magnitude: f64,
    source_direction: f64,
}

impl Wind {
    pub fn new(speed: f64, dir: f64) -> Self {
        Self {
            speed_magnitude: speed,
            source_direction: dir,
        }
    }
    pub fn speed_magnitude(&self) -> f64 {
        self.speed_magnitude
    }
    pub fn source_direction(&self) -> f64 {
        self.source_direction
    }
    pub fn component_direction(&self) -> (f64, f64) {
        (
            self.source_direction.to_radians().cos(),
            self.source_direction.to_radians().sin(),
        )
    }
    pub fn component_velocity(&self) -> (f64, f64) {
        let (north, east) = self.component_direction();
        (self.speed_magnitude * north, self.speed_magnitude * east)
    }
}

#[derive(Debug)]
pub struct RapidWind {
    pub timestamp: DateTime<Utc>,
    pub wind: Wind,
}

impl From<reader::RawRapidWind> for RapidWind {
    fn from(raw: reader::RawRapidWind) -> Self {
        Self {
            timestamp: DateTime::from_utc(NaiveDateTime::from_timestamp(raw.ob.0, 0), Utc),
            wind: Wind::new(raw.ob.1, raw.ob.2),
        }
    }
}

#[derive(Debug)]
pub enum PrecipType {
    None,
    Rain,
    Hail,
    RainHail,
}

#[derive(Debug)]
pub struct Observation {
    pub timestamp: DateTime<Utc>,
    pub wind_lull: Wind,
    pub wind_avg: Wind,
    pub wind_gust: Wind,
    pub wind_interval: Duration,
    pub station_pressure: f64,
    pub air_temperature: f64,
    pub relative_humidity: f64,
    pub illuminance: f64,
    pub ultraviolet_index: f64,
    pub solar_irradiance: f64,
    pub rain_last_minute: f64,
    pub precipitation_type: PrecipType,
    pub lightning_average_distance: f64,
    pub lightning_count: i64,
    pub battery_volts: f64,
    pub report_interval: Duration,
}

const LAMBDA: f64 = -0.0065; // Temperature lapse rate (K m^-1)
const R_SUB_D: f64 = 287.0; // Specific gas constant of dry air (J kg^-1 K^-1)
const G: f64 = 9.80665; // Gravitational constant (m s^-2)
const G_OVER_RD_LAMBDA: f64 = -G / (R_SUB_D * LAMBDA);
const ZERO_C_KELVIN: f64 = 273.15;

impl Observation {
    pub fn barometric_pressure(&self, station_elevation: f64) -> f64 {
        let t_kelvin = self.air_temperature + ZERO_C_KELVIN;
        let ratio = (1.0 + (LAMBDA * station_elevation) / (t_kelvin - LAMBDA * station_elevation))
            .powf(-G_OVER_RD_LAMBDA);
        self.station_pressure * ratio
    }
}

impl TryFrom<reader::RawObservation> for Observation {
    type Error = (reader::RawObservation, anyhow::Error);
    fn try_from(raw: reader::RawObservation) -> Result<Self, Self::Error> {
        let timestamp = raw.obs[0][0] as i64;
        let wind_lull = raw.obs[0][1];
        let wind_avg = raw.obs[0][2];
        let wind_gust = raw.obs[0][3];
        let wind_dir = raw.obs[0][4];
        Ok(Self {
            timestamp: DateTime::from_utc(NaiveDateTime::from_timestamp(timestamp, 0), Utc),
            wind_lull: Wind::new(wind_lull, wind_dir),
            wind_avg: Wind::new(wind_avg, wind_dir),
            wind_gust: Wind::new(wind_gust, wind_dir),
            wind_interval: Duration::seconds(raw.obs[0][5] as i64),
            station_pressure: raw.obs[0][6],
            air_temperature: raw.obs[0][7],
            relative_humidity: raw.obs[0][8],
            illuminance: raw.obs[0][9],
            ultraviolet_index: raw.obs[0][10],
            solar_irradiance: raw.obs[0][11],
            rain_last_minute: raw.obs[0][12],
            precipitation_type: match raw.obs[0][13] as i64 {
                0 => PrecipType::None,
                1 => PrecipType::Rain,
                2 => PrecipType::Hail,
                3 => PrecipType::RainHail,
                other => return Err((raw, anyhow!("Unrecognized precip type {}", other))),
            },
            lightning_average_distance: raw.obs[0][14],
            lightning_count: raw.obs[0][15] as i64,
            battery_volts: raw.obs[0][16],
            report_interval: Duration::minutes(raw.obs[0][17] as i64),
        })
    }
}

#[derive(Debug)]
pub struct SensorStatus {
    pub lightning_failure: bool,
    pub lightning_noise: bool,
    pub lightning_disturber: bool,
    pub pressure_failed: bool,
    pub temperature_failed: bool,
    pub humidity_failed: bool,
    pub wind_failed: bool,
    pub precip_failed: bool,
    pub irradiance_failed: bool,
    pub power_booster_depleted: bool,
    pub power_booster_shore_power: bool,
}

impl From<u32> for SensorStatus {
    fn from(field: u32) -> Self {
        Self {
            lightning_failure: field & 0b1 != 0,
            lightning_noise: field & 0b10 != 0,
            lightning_disturber: field & 0b100 != 0,
            pressure_failed: field & 0b1000 != 0,
            temperature_failed: field & 0b10000 != 0,
            humidity_failed: field & 0b100000 != 0,
            wind_failed: field & 0b1000000 != 0,
            precip_failed: field & 0b10000000 != 0,
            irradiance_failed: field & 0b100000000 != 0,
            power_booster_depleted: field & 0x8000 != 0,
            power_booster_shore_power: field & 0x10000 != 0,
        }
    }
}

#[derive(Debug)]
pub struct DeviceStatus {
    pub serial_number: String,
    pub hub_serial_number: String,
    pub timestamp: DateTime<Utc>,
    pub uptime: Duration,
    pub voltage: f64,
    pub firmware_revision: i32,
    pub rssi: f64,
    pub hub_rssi: f64,
    pub sensor_status: SensorStatus,
    pub debug: bool,
}

impl From<reader::RawDeviceStatus> for DeviceStatus {
    fn from(raw: reader::RawDeviceStatus) -> Self {
        Self {
            serial_number: raw.serial_number,
            hub_serial_number: raw.hub_sn,
            timestamp: DateTime::from_utc(NaiveDateTime::from_timestamp(raw.timestamp, 0), Utc),
            uptime: Duration::seconds(raw.uptime),
            voltage: raw.voltage,
            firmware_revision: raw.firmware_revision,
            rssi: raw.rssi,
            hub_rssi: raw.hub_rssi,
            sensor_status: raw.sensor_status.into(),
            debug: raw.debug == 1,
        }
    }
}

#[derive(Debug, Default)]
pub struct ResetFlags {
    pub brownout: bool,
    pub pin: bool,
    pub power_on: bool,
    pub software: bool,
    pub watchdog: bool,
    pub window_watchdog: bool,
    pub low_power: bool,
    pub hard_fault: bool,
}

impl FromStr for ResetFlags {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut new = Self::default();
        for label in s.split(',') {
            match label {
                "BOR" => new.brownout = true,
                "PIN" => new.pin = true,
                "POR" => new.power_on = true,
                "SFT" => new.software = true,
                "WDG" => new.watchdog = true,
                "WWD" => new.window_watchdog = true,
                "LPW" => new.low_power = true,
                "HRDFLT" => new.hard_fault = true,
                label => bail!("Unrecognized reset flag label {}", label),
            }
        }
        Ok(new)
    }
}

#[derive(Debug)]
pub struct HubStatus {
    pub serial_number: String,
    pub firmware_revision: String,
    pub uptime: Duration,
    pub rssi: f64,
    pub timestamp: DateTime<Utc>,
    pub reset_flags: ResetFlags,
    pub seq: i32,
}

impl TryFrom<reader::RawHubStatus> for HubStatus {
    type Error = (reader::RawHubStatus, anyhow::Error);
    fn try_from(raw: reader::RawHubStatus) -> Result<Self, Self::Error> {
        let reset_flags = match raw.reset_flags.parse() {
            Ok(v) => v,
            Err(e) => return Err((raw, e)),
        };
        Ok(Self {
            serial_number: raw.serial_number,
            firmware_revision: raw.firmware_revision,
            uptime: Duration::seconds(raw.uptime),
            rssi: raw.rssi,
            timestamp: DateTime::from_utc(NaiveDateTime::from_timestamp(raw.uptime, 0), Utc),
            reset_flags,
            seq: raw.seq,
        })
    }
}

pub fn new<RD: Stream<Item = RawTempestMsg>>(reader: RD) -> impl Stream<Item = TempestMsg> {
    reader.filter_map(|raw| {
        raw.try_into()
            .map_err(|(raw, e)| {
                warn!("Dropped undecodable message: {:?}", raw);
                warn!(".. error was: {}", e);
            })
            .ok()
    })
}
