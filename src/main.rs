use anyhow::Context;
use log::{info, debug, LevelFilter};
use simple_logger::SimpleLogger;
use structopt::StructOpt;
use tokio::net::UdpSocket;

#[derive(StructOpt, Debug)]
struct Opt {
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

    let input_sock = UdpSocket::bind("0.0.0.0:50222").await?;
    let mut buf = [0; 1024];
    loop {
        let (len, addr) = input_sock.recv_from(&mut buf).await?;
        let json = std::str::from_utf8(&buf[..len])?;
        debug!("{}", json);
    }
}
