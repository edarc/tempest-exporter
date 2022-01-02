use std::sync::Mutex;

use log::{debug, error, info};
use rumqttc::{
    AsyncClient, Event as MqEvent, Incoming as MqIncoming, MqttOptions, Outgoing as MqOutgoing, QoS,
};
use tokio::sync::{mpsc, oneshot};

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
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl Publisher {
    pub fn new(station_params: StationParams, mqtt_params: MqttParams) -> Self {
        let (message_tx, message_rx) = mpsc::channel(1024);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        if mqtt_params.mqtt_broker.is_some() {
            Self::start_actual(mqtt_params, message_rx, shutdown_rx);
        } else {
            Self::start_dummy(message_rx, shutdown_rx);
        }

        Self {
            station_params,
            sender: MsgSender(message_tx),
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
        }
    }

    fn start_actual(
        mqtt_params: MqttParams,
        mut message_rx: mpsc::Receiver<Message>,
        shutdown_rx: oneshot::Receiver<()>,
    ) {
        let mut mqtt_options = MqttOptions::new(
            "tempest-exporter",
            mqtt_params.mqtt_broker.unwrap(), // Checked by caller
            mqtt_params.mqtt_port,
        );
        mqtt_options.set_keep_alive(std::time::Duration::from_secs(15));
        if let (Some(user), Some(pass)) = (mqtt_params.mqtt_username, mqtt_params.mqtt_password) {
            mqtt_options.set_credentials(user, pass);
        }

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(MqEvent::Incoming(MqIncoming::Disconnect))
                    | Ok(MqEvent::Outgoing(MqOutgoing::Disconnect)) => {
                        info!("MQTT graceful disconnect");
                        break;
                    }
                    Ok(MqEvent::Incoming(MqIncoming::ConnAck(_))) => {
                        info!("MQTT connection established")
                    }
                    Ok(notif) => debug!("MQTT: {:?}", notif),
                    Err(e) => {
                        error!("MQTT: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            }
        });
        let publisher_task = tokio::spawn({
            let client = client.clone();
            async move {
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
            }
        });
        tokio::spawn(async move {
            shutdown_rx.await.ok();
            info!("MQTT publisher stopping");
            publisher_task.abort();
            client.disconnect().await.ok();
        });
    }

    fn start_dummy(mut message_rx: mpsc::Receiver<Message>, shutdown_rx: oneshot::Receiver<()>) {
        let dummy_sink_task = tokio::spawn(async move {
            loop {
                if let Some((topic, _, payload)) = message_rx.recv().await {
                    debug!("DUMMY: {} -> {}", topic, payload);
                }
            }
        });
        tokio::spawn(async move {
            shutdown_rx.await.ok();
            dummy_sink_task.abort();
        });
    }

    pub fn shutdown(&self) {
        self.shutdown_tx
            .lock()
            .unwrap()
            .take()
            .map(|stx| stx.send(()));
    }

    pub fn handle_report(&self, msg: &decoder::TempestMsg) {
        use decoder::TempestMsg as TM;
        match msg {
            TM::PrecipEvent(pe) => pe.publish_to(&self.sender, &self.station_params),
            TM::StrikeEvent(se) => se.publish_to(&self.sender, &self.station_params),
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

impl PublishTo for decoder::PrecipEvent {
    fn publish_to(&self, sender: &MsgSender, _station_params: &StationParams) {
        sender.send("tempest/event/precip", false, self.timestamp.to_rfc3339());
    }
}

impl PublishTo for decoder::StrikeEvent {
    fn publish_to(&self, sender: &MsgSender, _station_params: &StationParams) {
        sender.send(
            "tempest/event/lightning",
            false,
            serde_json::to_string(&self).unwrap(),
        );
    }
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
