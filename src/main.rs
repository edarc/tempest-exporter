mod decoder;
mod exporter;
mod reader;
mod receiver;

use anyhow::Context;
use log::{debug, info, LevelFilter};
use simple_logger::SimpleLogger;
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[derive(StructOpt, Clone, Debug)]
pub struct StationParams {
    /// Station elevation in meters - used to compute barometric pressure.
    #[structopt(long = "station-elevation")]
    pub elevation: f64,
}

#[derive(StructOpt, Debug)]
struct Opt {
    /// Station parameters
    #[structopt(flatten)]
    station_params: StationParams,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .with_utc_timestamps()
        .init()
        .context("Logging setup failed")
        .unwrap();
    info!("Starting Tempest exporter");

    let rx = receiver::Receiver::new().await?;
    let rdr = reader::new(rx);
    let mut dec = decoder::new(rdr);

    let exporter = exporter::Exporter::new(opt.station_params.clone());

    while let Some(msg) = dec.next().await {
        exporter.handle_report(&msg);

        use decoder::TempestMsg as TM;
        match msg {
            TM::RapidWind(_) => exporter.dump(),
            _ => debug!("{:#?}", msg),
        }
    }
    Ok(())
}
