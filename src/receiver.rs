use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures_core::stream::Stream;
use log::warn;
use tokio::io::ReadBuf;
use tokio::net::UdpSocket;

pub struct Receiver(UdpSocket);

impl Receiver {
    pub async fn new() -> anyhow::Result<Self> {
        Ok(Receiver(UdpSocket::bind("0.0.0.0:50222").await?))
    }
}

impl Stream for Receiver {
    type Item = String;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buf = [0; 1024];
        let mut readbuf = ReadBuf::new(&mut buf);

        match self.0.poll_recv_from(cx, &mut readbuf) {
            Poll::Pending => Poll::Pending,

            Poll::Ready(Err(e)) => {
                warn!("Receiver terminated: socket error {}", e);
                Poll::Ready(None)
            }

            Poll::Ready(Ok(_)) => match std::str::from_utf8(readbuf.filled()) {
                Ok(json) => Poll::Ready(Some(json.to_string())),
                Err(e) => {
                    warn!("Receiver terminated: malformed JSON {}", e);
                    Poll::Ready(None)
                }
            },
        }
    }
}
