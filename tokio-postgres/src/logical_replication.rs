use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::simple_query;
use crate::{query, slice_iter, Error, Statement};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use futures::{ready, Stream};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use std::convert::TryInto;
use std::io::{self};
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

// Logical replication consists of two phases:
// 1. All changes from the start WAL position are streamed using CopyBoth messages.
// 2. Once 1 is done, any further changes are streamed as LogicalReplication messages.

// We need to provide an interface for clients to consume both phases.

pub struct Client<'a> {
    inner_client: &'a InnerClient,
}

impl<'a> Client<'a> {
    pub fn new(inner_client: &'a InnerClient) -> Self {
        Client { inner_client }
    }

    fn inner(&self) -> &InnerClient {
        &self.inner_client
    }

    // `query` is expected to be a START_REPLICATION command
    pub async fn start_replication(&self, query: &str) -> Result<LogicalReplicationStream, Error> {
        println!("executing logical replication query {}", query);

        // We have to use `simple_query` rather than `query` because we're in replication mode, which
        // doesn't allow prepared statements.
        let buf = simple_query::encode(self.inner(), &query)?;
        let mut responses = self
            .inner()
            .send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

        let resp = responses.next().await?;
        println!("Response: {:?}", resp.kind());

        match resp {
            Message::CopyBothResponse(_) => {}
            _ => return Err(Error::unexpected_message()),
        }

        Ok(LogicalReplicationStream {
            responses,
            _p: PhantomPinned,
            last_wal_received: None,
        })
    }

    pub fn send_keepalive(&self, last_wal_received: u64) -> Result<(), Error> {
        let update = standby_status_update(last_wal_received).map_err(Error::io)?;
        self.inner()
            .send(RequestMessages::Single(FrontendMessage::Raw(
                update.encode(),
            )))?;
        Ok(())
    }
}

pin_project! {
    /// A stream of logical WAL data.
    pub struct LogicalReplicationStream {
        last_wal_received: Option<u64>,
        pub responses: Responses,
        #[pin]
        pub _p: PhantomPinned,
    }
}

pub fn send_keepalive(client: &InnerClient, last_wal_received: u64) -> Result<(), Error> {
    let update = standby_status_update(last_wal_received).map_err(Error::io)?;
    client.send(RequestMessages::Single(FrontendMessage::Raw(
        update.encode(),
    )))?;
    Ok(())
}

const MICROS_BETWEEN_1970_AND_2000: u64 = 946684800000000;

fn standby_status_update(last_wal_received: u64) -> Result<StandbyStatusUpdate, io::Error> {
    let wal_plus_one = last_wal_received + 1;
    // Current timestamp, in microseconds since 2000
    let time_since_unix_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "error calculating system time"))?;
    let time_since_pg_epoch =
        time_since_unix_epoch - Duration::from_micros(MICROS_BETWEEN_1970_AND_2000);
    let micros_since_pg_epoch: u64 = time_since_pg_epoch
        .as_micros()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "system time is too large for u64"))?;
    Ok(StandbyStatusUpdate {
        last_wal_received: wal_plus_one,
        last_wal_flushed: wal_plus_one,
        last_wal_applied: wal_plus_one,
        timestamp: micros_since_pg_epoch,
        reply_immediately: false,
    })
}

// TODO: this should just stream raw logical WAL data, and handle keepalives automatically.
impl<'a> Stream for LogicalReplicationStream<'a> {
    type Item = Result<LogicalRepPayload, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // This loop allows us to discard any PrimaryKeepalive messages, only returning messages
        // with useful data.
        match ready!(this.responses.poll_next(cx)?) {
            Message::CopyData(body) => match parse_payload(body.into_bytes()) {
                Ok(payload) => Poll::Ready(Some(Ok(payload))),
                // Ok(LogicalRepPayload::XLogData { data, wal_end, .. }) => {
                //     println!("wal end: {:?}", wal_end);
                //     self.last_wal_received = Some(wal_end);
                //     println!("this.last_wal_received: {:?}", self.last_wal_received);
                //     return Poll::Ready(Some(Ok(data)));
                // }
                // Ok(LogicalRepPayload::PrimaryKeepalive {
                //     reply_immediately, ..
                // }) => {
                //     println!("Received keepalive message");
                //     if reply_immediately {
                //         if let Some(wal) = self.last_wal_received {
                //             println!("Replying to keepalive");
                //             let res =
                //                 standby_status_update(wal).map_err(Error::io).and_then(|s| {
                //                     self.client
                //                         .send(RequestMessages::Single(FrontendMessage::Raw(s)))
                //                 });
                //             if let Err(err) = res {
                //                 return Poll::Ready(Some(Err(err)));
                //             };
                //         }
                //     }
                // }
                Err(err) => Poll::Ready(Some(Err(Error::parse(err)))),
            },
            Message::CopyDone => Poll::Ready(None),
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}

fn parse_payload(buf: Bytes) -> io::Result<LogicalRepPayload> {
    println!("{:?}", buf);
    let tag = buf[0];
    match tag {
        XLOG_DATA_TAG => {
            if buf.len() < 25 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid message length ({} < 25)", buf.len()),
                ));
            }

            let wal_start = BigEndian::read_u64(&buf.slice(1..9));
            let wal_end = BigEndian::read_u64(&buf.slice(9..17));
            let timestamp = BigEndian::read_u64(&buf.slice(17..25));
            let data = buf.slice(25..);
            Ok(LogicalRepPayload::XLogData {
                wal_start,
                wal_end,
                timestamp,
                data,
            })
        }
        PRIMARY_KEEPALIVE_TAG => {
            if buf.len() < 18 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid message length ({} < 18)", buf.len()),
                ));
            }

            let wal_end = BigEndian::read_u64(&buf.slice(1..9));
            let timestamp = BigEndian::read_u64(&buf.slice(9..17));
            let reply_immediately = buf[17] == 1;
            Ok(LogicalRepPayload::PrimaryKeepalive {
                wal_end,
                timestamp,
                reply_immediately,
            })
        }
        tag => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown logical replication message tag `{:?}`", tag),
            ));
        }
    }
}

const XLOG_DATA_TAG: u8 = b'w';
const PRIMARY_KEEPALIVE_TAG: u8 = b'k';

#[derive(Debug, PartialEq, Eq)]
pub enum LogicalRepPayload {
    XLogData {
        wal_start: u64,
        wal_end: u64,
        timestamp: u64,
        data: Bytes,
    },
    PrimaryKeepalive {
        wal_end: u64,
        timestamp: u64,
        reply_immediately: bool,
    },
}

/// A message sent from the frontend to the backend in response to a PrimaryKeepalive message.
/// Each WAL location is actually one greater than the name suggests.
/// For example, `last_wal_received` is the location of the last WAL byte received, + 1.
pub struct StandbyStatusUpdate {
    last_wal_received: u64,
    last_wal_flushed: u64,
    last_wal_applied: u64,
    timestamp: u64,
    reply_immediately: bool,
}

impl StandbyStatusUpdate {
    fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(34);
        // Message identifier
        buf.extend_from_slice(b"r");
        // Last WAL byte written to disk
        BigEndian::write_u64(&mut buf, self.last_wal_received);
        // Last WAL byte flushed to disk
        BigEndian::write_u64(&mut buf, self.last_wal_flushed);
        // Last WAL byte applied
        BigEndian::write_u64(&mut buf, self.last_wal_applied);
        // Timestamp
        BigEndian::write_u64(&mut buf, self.timestamp);
        buf.into()
    }
}
