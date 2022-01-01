use futures_core::stream::Stream;
use log::warn;
use serde::Deserialize;
use tokio_stream::StreamExt;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum RawTempestMsg {
    #[serde(rename = "evt_precip")]
    PrecipEvent(RawPrecipEvent),
    #[serde(rename = "evt_strike")]
    StrikeEvent(RawStrikeEvent),
    #[serde(rename = "rapid_wind")]
    RapidWind(RawRapidWind),
    #[serde(rename = "obs_st")]
    Observation(RawObservation),
    #[serde(rename = "device_status")]
    DeviceStatus(RawDeviceStatus),
    #[serde(rename = "hub_status")]
    HubStatus(RawHubStatus),
}

#[derive(Deserialize, Debug)]
pub struct RawPrecipEvent {
    pub serial_number: String,
    pub hub_sn: String,
    pub evt: (i64,),
}

#[derive(Deserialize, Debug)]
pub struct RawStrikeEvent {
    pub serial_number: String,
    pub hub_sn: String,
    pub evt: (i64, f64, f64),
}

#[derive(Deserialize, Debug)]
pub struct RawRapidWind {
    pub serial_number: String,
    pub hub_sn: String,
    pub ob: (i64, f64, f64),
}

#[derive(Deserialize, Debug)]
pub struct RawObservation {
    pub serial_number: String,
    pub hub_sn: String,
    pub obs: [[f64; 18]; 1],
    pub firmware_revision: i32,
}

#[derive(Deserialize, Debug)]
pub struct RawDeviceStatus {
    pub serial_number: String,
    pub hub_sn: String,
    pub timestamp: i64,
    pub uptime: i64,
    pub voltage: f64,
    pub firmware_revision: i32,
    pub rssi: f64,
    pub hub_rssi: f64,
    pub sensor_status: u32,
    pub debug: i32,
}

#[derive(Deserialize, Debug)]
pub struct RawHubStatus {
    pub serial_number: String,
    pub firmware_revision: String,
    pub uptime: i64,
    pub rssi: f64,
    pub timestamp: i64,
    pub reset_flags: String,
    pub seq: i32,
    pub radio_stats: [i32; 5],
}

pub fn new<RX: Stream<Item = String>>(receiver: RX) -> impl Stream<Item = RawTempestMsg> {
    receiver.filter_map(|json| {
        serde_json::from_str(&json)
            .map_err(|e| {
                warn!("Dropped unreadable message: {}", json);
                warn!(".. error was: {}", e);
            })
            .ok()
    })
}
