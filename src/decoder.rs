use std::convert::TryFrom;
use std::str::FromStr;

use anyhow::{anyhow, bail};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use futures_core::stream::Stream;
use log::warn;
use serde::Serialize;
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

#[derive(Debug, Serialize)]
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
pub enum PrecipKind {
    None,
    Rain,
    Hail,
    RainHail,
}

#[derive(Debug)]
pub struct WindObservation {
    pub lull: Wind,
    pub avg: Wind,
    pub gust: Wind,
    pub interval: Duration,
}

#[derive(Debug)]
pub struct SolarObservation {
    pub illuminance: f64,
    pub ultraviolet_index: f64,
    pub irradiance: f64,
}

#[derive(Debug)]
pub struct PrecipObservation {
    pub quantity_last_minute: f64,
    pub kind: PrecipKind,
}

#[derive(Debug)]
pub struct LightningObservation {
    pub average_distance: f64,
    pub count: i64,
}

#[derive(Debug)]
pub struct Observation {
    pub timestamp: DateTime<Utc>,
    pub wind: Option<WindObservation>,
    pub station_pressure: Option<f64>,
    pub air_temperature: Option<f64>,
    pub relative_humidity: Option<f64>,
    pub solar: Option<SolarObservation>,
    pub precip: Option<PrecipObservation>,
    pub lightning: Option<LightningObservation>,
    pub battery_volts: f64,
    pub report_interval: Duration,
}

const LAMBDA: f64 = -0.0065; // Temperature lapse rate (K m^-1)
const R_SUB_D: f64 = 287.0; // Specific gas constant of dry air (J kg^-1 K^-1)
const G: f64 = 9.80665; // Gravitational constant (m s^-2)
const G_OVER_RD_LAMBDA: f64 = -G / (R_SUB_D * LAMBDA);
const ZERO_C_KELVIN: f64 = 273.15;

// Opaque constants for Arden-Buck best-fit formula for saturated vapor pressure.
const ARDEN_BUCK_A: f64 = 6.1121;
const ARDEN_BUCK_B: f64 = 18.678;
const ARDEN_BUCK_C: f64 = 257.14;
const ARDEN_BUCK_D: f64 = 234.5;

// Opaque constants for Stull best-fit formula for wet bulb temperature.
const STULL_A: f64 = 0.151977;
const STULL_B: f64 = 8.313659;
const STULL_C: f64 = -1.676311;
const STULL_D: f64 = 0.00391838;
const STULL_E: f64 = 0.023101;
const STULL_F: f64 = -4.686035;

// Opaque constants for Steadman apparent temperature (radiation-incorporating).
const STEADMAN_CE: f64 = 0.348;
const STEADMAN_CWS: f64 = -0.70;
const STEADMAN_CQ: f64 = 0.70;
const STEADMAN_OWS: f64 = 10.0;
const STEADMAN_B: f64 = -4.25;

impl Observation {
    pub fn barometric_pressure(&self, station_elevation: f64) -> Option<f64> {
        let t_kelvin = self.air_temperature? + ZERO_C_KELVIN;
        let ratio = (1.0 + (LAMBDA * station_elevation) / (t_kelvin - LAMBDA * station_elevation))
            .powf(-G_OVER_RD_LAMBDA);
        Some(self.station_pressure? * ratio)
    }

    pub fn vapor_pressure_saturated(&self) -> Option<f64> {
        let t = self.air_temperature?;
        Some(ARDEN_BUCK_A * ((ARDEN_BUCK_B - t / ARDEN_BUCK_D) * (t / (ARDEN_BUCK_C + t))).exp())
    }

    pub fn vapor_pressure_actual(&self) -> Option<f64> {
        Some(self.vapor_pressure_saturated()? * (self.relative_humidity? / 100.0))
    }

    pub fn dew_point(&self) -> Option<f64> {
        let ln_pa_t_over_a = (self.vapor_pressure_actual()? / ARDEN_BUCK_A).ln();
        Some(ARDEN_BUCK_C * ln_pa_t_over_a / (ARDEN_BUCK_B - ln_pa_t_over_a))
    }

    pub fn wet_bulb_temperature(&self) -> Option<f64> {
        let t = self.air_temperature?;
        let rh = self.relative_humidity?;
        Some(
            t * (STULL_A * (rh + STULL_B).sqrt()).atan() + (t + rh).atan() - (rh + STULL_C).atan()
                + STULL_D * rh.powf(3.0 / 2.0) * (STULL_E * rh).atan()
                + STULL_F,
        )
    }

    pub fn apparent_temperature(&self) -> Option<f64> {
        let ta = self.air_temperature?;
        let e = self.vapor_pressure_actual()?;
        let ws = self.wind.as_ref()?.avg.speed_magnitude();
        let q = self.solar.as_ref()?.irradiance;
        Some(
            ta + STEADMAN_CE * e
                + STEADMAN_CWS * ws
                + (STEADMAN_CQ * q) / (ws + STEADMAN_OWS)
                + STEADMAN_B,
        )
    }
}

impl TryFrom<reader::RawObservation> for Observation {
    type Error = (reader::RawObservation, anyhow::Error);
    fn try_from(raw: reader::RawObservation) -> Result<Self, Self::Error> {
        let timestamp = match raw.obs[0][0] {
            Some(unix_sec) => {
                DateTime::from_utc(NaiveDateTime::from_timestamp(unix_sec as i64, 0), Utc)
            }
            None => return Err((raw, anyhow!("Missing observation timestamp"))),
        };

        let wind: Option<WindObservation> = (|| {
            let wind_dir = raw.obs[0][4]?;
            Some(WindObservation {
                lull: Wind::new(raw.obs[0][1]?, wind_dir),
                avg: Wind::new(raw.obs[0][2]?, wind_dir),
                gust: Wind::new(raw.obs[0][3]?, wind_dir),
                interval: Duration::seconds(raw.obs[0][5]? as i64),
            })
        })();

        let solar: Option<SolarObservation> = (|| {
            Some(SolarObservation {
                illuminance: raw.obs[0][9]?,
                ultraviolet_index: raw.obs[0][10]?,
                irradiance: raw.obs[0][11]?,
            })
        })();

        let precip_obs: Option<(f64, i64)> = (|| Some((raw.obs[0][12]?, raw.obs[0][13]? as i64)))();
        let precip = if let Some((qty, kind_raw)) = precip_obs {
            Some(PrecipObservation {
                quantity_last_minute: qty,
                kind: match kind_raw {
                    0 => PrecipKind::None,
                    1 => PrecipKind::Rain,
                    2 => PrecipKind::Hail,
                    3 => PrecipKind::RainHail,
                    other => return Err((raw, anyhow!("Unrecognized precip type {}", other))),
                },
            })
        } else {
            None
        };

        let lightning = (|| {
            Some(LightningObservation {
                average_distance: raw.obs[0][14]?,
                count: raw.obs[0][15]? as i64,
            })
        })();

        Ok(Self {
            timestamp,
            wind,
            station_pressure: raw.obs[0][6],
            air_temperature: raw.obs[0][7],
            relative_humidity: raw.obs[0][8],
            solar,
            precip,
            lightning,
            battery_volts: match raw.obs[0][16] {
                Some(volts) => volts,
                None => return Err((raw, anyhow!("Missing battery voltage"))),
            },
            report_interval: match raw.obs[0][17] {
                Some(interval) => Duration::minutes(interval as i64),
                None => return Err((raw, anyhow!("Missing report interval"))),
            },
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
