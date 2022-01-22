#![allow(missing_docs)]
#![allow(unused)]

use bytes::{Bytes, BytesMut};

pub enum LogicalRepMessage {
    Begin(BeginBody),
    Message(MessageBody),
    Commit(CommitBody),
    Origin(OriginBody),
    Relation(RelationBody),
    Type(TypeBody),
    Insert(InsertBody),
    Update(UpdateBody),
    Delete(DeleteBody),
    Truncate(TruncateBody),
    StreamStart(),
    StreamCommit(StreamCommitBody),
    StreamAbort(StreamAbortBody),
}

pub struct BeginBody {
    lsn: LSN,
    timestamp: Timestamp,
    xid: Xid,
}

pub struct MessageBody {
    xid: Xid,
    flags: u8,
    lsn: LSN,
    prefix: String,
    content_length: u32,
    content: Bytes,
}

pub struct CommitBody {
    commit_lsn: LSN,
    transaction_end_lsn: LSN,
    timestamp: Timestamp,
}

pub struct OriginBody {
    lsn: LSN,
    name: String,
}

pub struct RelationBody {
    transaction_xid: Xid,
    relation_id: ID,
    namespace: String,
    relation_name: String,
    replica_identity_setting: u8,
    columns: Vec<ColumnInfo>,
}

pub struct ColumnInfo {
    flags: u8,
    name: String,
    data_type_id: ID,
    type_modifier: u32,
}

pub struct TypeBody {
    transaction_xid: Xid,
    id: ID,
    namespace: String,
    name: String,
}

pub struct InsertBody {
    transaction_xid: Xid,
    relation_id: ID,
    data: TupleData,
}

pub struct UpdateBody {
    transaction_xid: Xid,
    relation_id: ID,
    old_tuple: Option<OldTupleData>,
    data: TupleData,
}

pub struct DeleteBody {
    transaction_xid: Xid,
    relation_id: ID,
    old_tuple: Option<OldTupleData>,
    data: TupleData,
}

pub enum OldTupleData {
    // The update/delete changed data in columns that are part of the REPLICA IDENTITY index.
    // The contained tuple data is the old primary key.
    Key(TupleData),
    // The old tuple. Sent because the table has REPLICA IDENTITY set to FULL.
    Tuple(TupleData),
}

pub struct TupleData(Vec<(DataFormat, Bytes)>);

/// Describes the format of the value contained in a tuple element.
pub enum DataFormat {
    /// The value is null.
    Null,
    /// The value is an (unchanged) TOAST value.
    /// In this case the actual value is not included in the message.
    UnchangedToast,
    /// The value is formatted as text.
    Text,
    /// The value is formatted as binary.
    Binary,
}

pub struct TruncateBody {
    transaction_xid: Xid,
    options: TruncateOptions,
    relations: Vec<ID>,
}

pub enum TruncateOptions {
    Cascade(),
    RestartIdentity(),
}

pub struct StreamCommitBody {
    transaction_xid: Xid,
    commit_lsn: LSN,
    transaction_end_lsn: LSN,
    timestamp: Timestamp,
}

pub struct StreamAbortBody {
    transaction_xid: Xid,
    subtransaction_xid: Xid,
}

/// A location in the write-ahead log
pub struct LSN(u64);

/// A timestamp, in microseconds since 2021-01-01
pub struct Timestamp(u64);

/// A transaction ID
pub struct Xid(u32);

/// A relation ID
pub struct ID(u32);
