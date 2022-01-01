use log::{debug, error, info};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use tokio::sync::mpsc;

use crate::decoder;
use crate::{MqttParams, StationParams};

type Message = (String, bool, String);

struct MsgSender(mpsc::Sender<Message>);

impl MsgSender {
    fn send(&self, topic: impl std::borrow::Borrow<str>, retain: bool, payload: String) {
        self.0
            .try_send((topic.borrow().to_string(), retain, payload))
            .ok();
    }
}

pub struct Publisher {
    station_params: StationParams,
    sender: MsgSender,
}

impl Publisher {
    pub fn new(station_params: StationParams, mqtt_params: MqttParams) -> Self {
        let (message_tx, mut message_rx) = mpsc::channel(1024);

        let mut mqtt_options = MqttOptions::new(
            "tempest-exporter",
            mqtt_params.mqtt_broker,
            mqtt_params.mqtt_port,
        );
        mqtt_options.set_keep_alive(std::time::Duration::from_secs(15));
        mqtt_options.set_credentials(mqtt_params.mqtt_username, mqtt_params.mqtt_password);

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(notif) => debug!("MQTT: {:?}", notif),
                    Err(e) => {
                        error!("MQTT: {}", e);
                        break;
                    }
                }
            }
        });
        tokio::spawn(async move {
            loop {
                if let Some((topic, retain, payload)) = message_rx.recv().await {
                    match client
                        .publish(topic, QoS::AtLeastOnce, retain, payload)
                        .await
                    {
                        Ok(()) => {}
                        Err(e) => error!("MQTT publish failed: {}", e),
                    }
                }
            }
        });

        Self {
            station_params,
            sender: MsgSender(message_tx),
        }
    }

    pub fn handle_report(&self, msg: &decoder::TempestMsg) {
        use decoder::TempestMsg as TM;
        match msg {
            //TM::PrecipEvent(pe) => pe.publish_to(&self.sender, &self.station_params),
            //TM::StrikeEvent(se) => se.publish_to(&self.sender, &self.station_params),
            TM::RapidWind(rw) => rw.publish_to(&self.sender, &self.station_params),
            TM::Observation(obs) => obs.publish_to(&self.sender, &self.station_params),
            //TM::DeviceStatus(ds) => ds.publish_to(&self.sender, &self.station_params),
            //TM::HubStatus(hs) => hs.publish_to(&self.sender, &self.station_params),
            _ => {}
        }
    }
}

fn publish_wind(sender: &MsgSender, prefix: &str, wind: &decoder::Wind) {
    sender.send(
        format!("{}/speed_magnitude_m_per_s", prefix),
        true,
        wind.speed_magnitude().to_string(),
    );
    sender.send(
        format!("{}/source_direction_deg", prefix),
        true,
        wind.source_direction().to_string(),
    );
    let (north, east) = wind.component_velocity();
    sender.send(
        format!("{}/component_velocity_m_per_s", prefix),
        true,
        format!("{} {}", north, east),
    );
}

trait PublishTo {
    fn publish_to(&self, sender: &MsgSender, station_params: &StationParams);
}

impl PublishTo for decoder::RapidWind {
    fn publish_to(&self, sender: &MsgSender, _station_params: &StationParams) {
        publish_wind(sender, "tempest/instant_wind", &self.wind);
    }
}

impl PublishTo for decoder::Observation {
    fn publish_to(&self, sender: &MsgSender, station_params: &StationParams) {
        sender.send(
            "tempest/observation/timestamp",
            true,
            self.timestamp.to_rfc3339(),
        );
        publish_wind(sender, "tempest/observation/wind/lull", &self.wind_lull);
        publish_wind(sender, "tempest/observation/wind/avg", &self.wind_avg);
        publish_wind(sender, "tempest/observation/wind/gust", &self.wind_gust);
        sender.send(
            "tempest/observation/pressure/station_hpa",
            true,
            self.station_pressure.to_string(),
        );
        sender.send(
            "tempest/observation/pressure/barometric_hpa",
            true,
            self.barometric_pressure(station_params.elevation)
                .to_string(),
        );
        sender.send(
            "tempest/observation/thermal/temperature_deg_c",
            true,
            self.air_temperature.to_string(),
        );
        sender.send(
            "tempest/observation/thermal/relative_humidity_pct",
            true,
            self.relative_humidity.to_string(),
        );
        sender.send(
            "tempest/observation/thermal/dew_point_deg_c",
            true,
            self.dew_point().to_string(),
        );
        sender.send(
            "tempest/observation/thermal/wet_bulb_temperature_deg_c",
            true,
            self.wet_bulb_temperature().to_string(),
        );
        sender.send(
            "tempest/observation/thermal/apparent_temperature_deg_c",
            true,
            self.apparent_temperature().to_string(),
        );
        sender.send(
            "tempest/observation/solar/illuminance_lux",
            true,
            self.illuminance.to_string(),
        );
        sender.send(
            "tempest/observation/solar/irradiance_w_per_m2",
            true,
            self.irradiance.to_string(),
        );
        sender.send(
            "tempest/observation/solar/uv_index",
            true,
            self.ultraviolet_index.to_string(),
        );
        sender.send(
            "tempest/observation/precip/previous_minute_rain_mm",
            true,
            self.rain_last_minute.to_string(),
        );
    }
}
