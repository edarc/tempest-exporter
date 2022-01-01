mod decoder;
mod exporter;
mod publisher;
mod reader;
mod receiver;

use std::sync::Arc;

use anyhow::Context;
use log::{error, info, LevelFilter};
use simple_logger::SimpleLogger;
use structopt::StructOpt;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use warp::Filter;

#[derive(StructOpt, Clone, Debug)]
pub struct StationParams {
    /// Station elevation in meters - used to compute barometric pressure.
    #[structopt(long = "station-elevation")]
    pub elevation: f64,
}

#[derive(StructOpt, Debug)]
pub struct MqttParams {
    /// Port to use for MQTT broker
    #[structopt(long, default_value = "1883")]
    mqtt_port: u16,

    /// Address of MQTT broker
    #[structopt(long)]
    mqtt_broker: String,

    /// MQTT username
    #[structopt(long)]
    mqtt_username: String,

    /// MQTT password
    #[structopt(long)]
    mqtt_password: String,
}

#[derive(StructOpt, Debug)]
struct Opt {
    /// Port to bind the Prometheus metrics server
    #[structopt(long, default_value = "8080")]
    metric_port: u16,

    /// MQTT parameters
    #[structopt(flatten)]
    mqtt_params: MqttParams,

    /// Station parameters
    #[structopt(flatten)]
    station_params: StationParams,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .with_utc_timestamps()
        .init()
        .context("Logging setup failed")
        .unwrap();
    info!("Starting Tempest exporter");

    let rx = receiver::Receiver::new().await?;
    let rdr = reader::new(rx);
    let mut dec = decoder::new(rdr);

    let exporter = Arc::new(exporter::Exporter::new(opt.station_params.clone()));
    let publisher = publisher::Publisher::new(opt.station_params.clone(), opt.mqtt_params);

    let mut alive = false;

    while let Some(msg) = dec.next().await {
        exporter.handle_report(&msg);
        publisher.handle_report(&msg);
        if !alive {
            alive = true;
            info!("Tempest API is alive");
        }
        if let decoder::TempestMsg::Observation(_) = msg {
            info!("First observation received; going online");
            break;
        }
    }

    let server_filter_chain = warp::path("healthz")
        .map(|| "ok")
        .or(warp::path("metrics").map({
            let ex = exporter.clone();
            move || {
                http::Response::builder()
                    .header("content-type", "text/plain; charset=utf-8")
                    .body(ex.encode())
            }
        }));
    let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(
        warp::serve(server_filter_chain)
            .bind_with_graceful_shutdown(([0, 0, 0, 0], opt.metric_port), async move {
                server_shutdown_rx.await.ok();
            })
            .1,
    );

    let (message_pump_shutdown_tx, mut message_pump_shutdown_rx) = oneshot::channel();
    let message_pump = tokio::spawn(async move {
        loop {
            if let Some(msg) = dec.next().await {
                exporter.handle_report(&msg);
                publisher.handle_report(&msg);
            } else {
                break;
            }
            if message_pump_shutdown_rx.try_recv().is_ok() {
                break;
            }
        }
    });

    tokio::select! {
        result = server => match result {
            Err(e) => error!("Server task panic: {}", e),
            Ok(()) => info!("Server task exited"),
        },
        result = message_pump => match result {
            Err(e) => error!("Exporter task panic: {}", e),
            Ok(()) => info!("Exporter task exited"),
        },
    }

    server_shutdown_tx.send(()).ok();
    message_pump_shutdown_tx.send(()).ok();

    Ok(())
}
