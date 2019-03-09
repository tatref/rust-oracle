// Rust-oracle - Rust binding for Oracle database
//
// URL: https://github.com/kubo/rust-oracle
//
//-----------------------------------------------------------------------------
// Copyright (c) 2017-2019 Kubo Takehiro <kubo@jiubao.org>. All rights reserved.
// This program is free software: you can modify it and/or redistribute it
// under the terms of:
//
// (i)  the Universal Permissive License v 1.0 or at your option, any
//      later version (http://oss.oracle.com/licenses/upl); and/or
//
// (ii) the Apache License v 2.0. (http://www.apache.org/licenses/LICENSE-2.0)
//-----------------------------------------------------------------------------

use super::OpCode;
use binding::*;
use error::error_from_dpi_error;
use private;
use std::borrow::Cow;
use std::slice;
use to_cow_str;
use Error;
use Result;

pub type Callback = Box<dyn FnMut(Result<Event>)>;

pub enum Protocol {
    Callback(Callback),
    Mail(String),
    PlSql(String),
    Http(String),
}

impl Protocol {
    pub fn callback<C>(callback: C) -> Protocol
    where
        C: FnMut(Result<Event>) + 'static,
    {
        Protocol::Callback(Box::new(callback))
    }
}

impl private::Sealed for Protocol {}

impl super::Protocol for Protocol {
    fn init_form(self, form: &mut super::SubscriptionForm) {
        form.namespace = DPI_SUBSCR_NAMESPACE_AQ;
        match self {
            Protocol::Callback(cb) => {
                form.protocol = DPI_SUBSCR_PROTO_CALLBACK as dpiSubscrProtocol;
                form.callback = Some(super::Callback::CQN(cb));
            }
            Protocol::Mail(rec) => {
                form.protocol = DPI_SUBSCR_PROTO_MAIL as dpiSubscrProtocol;
                form.recipient = Some(rec);
            }
            Protocol::PlSql(rec) => {
                form.protocol = DPI_SUBSCR_PROTO_PLSQL as dpiSubscrProtocol;
                form.recipient = Some(rec);
            }
            Protocol::Http(rec) => {
                form.protocol = DPI_SUBSCR_PROTO_HTTP as dpiSubscrProtocol;
                form.recipient = Some(rec);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum EventType {
    ObjectChange,
    QueryResultChange,
    Startup,
    Shutdown,
    ShutdownAny,
    Deregister,
}

impl EventType {
    fn from_dpi(val: dpiEventType) -> Result<EventType> {
        Ok(match val {
            DPI_EVENT_OBJCHANGE => EventType::ObjectChange,
            DPI_EVENT_QUERYCHANGE => EventType::QueryResultChange,
            DPI_EVENT_STARTUP => EventType::Startup,
            DPI_EVENT_SHUTDOWN => EventType::Shutdown,
            DPI_EVENT_SHUTDOWN_ANY => EventType::ShutdownAny,
            DPI_EVENT_DEREG => EventType::Deregister,
            _ => {
                return Err(Error::InternalError(format!(
                    "Unsupported subscription event type {}",
                    val
                )));
            }
        })
    }
}

#[derive(Debug)]
pub struct Event<'a> {
    event_type: EventType,
    database: Cow<'a, str>,
    tables: Vec<Table<'a>>,
    queries: Vec<Query<'a>>,
    transaction_id: &'a [u8],
    registered: bool,
}

impl<'a> Event<'a> {
    pub(crate) unsafe fn new(msg: &dpiSubscrMessage) -> Result<Event> {
        if msg.errorInfo.is_null() {
            Ok(Event {
                event_type: EventType::from_dpi(msg.eventType).unwrap(),
                database: to_cow_str(msg.dbName, msg.dbNameLength),
                tables: dpi_to_tables(msg.tables, msg.numTables),
                queries: dpi_to_queries(msg.queries, msg.numQueries),
                transaction_id: to_trans_id(msg),
                registered: msg.registered != 0,
            })
        } else {
            Err(error_from_dpi_error(&*msg.errorInfo))
        }
    }

    pub fn event_type(&self) -> &EventType {
        &self.event_type
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    pub fn queries(&self) -> &[Query] {
        &self.queries
    }

    pub fn transaction_id(&self) -> &[u8] {
        &self.transaction_id
    }

    pub fn registered(&self) -> bool {
        self.registered
    }
}

unsafe fn to_trans_id(msg: &dpiSubscrMessage) -> &[u8] {
    slice::from_raw_parts(msg.txId as *mut u8, msg.txIdLength as usize)
}

unsafe fn dpi_to_tables<'a>(tables: *mut dpiSubscrMessageTable, num: u32) -> Vec<Table<'a>> {
    let mut vec = Vec::with_capacity(num as usize);
    for i in 0..(num as isize) {
        vec.push(Table::from_dpi(&*tables.offset(i)));
    }
    vec
}

unsafe fn dpi_to_queries<'a>(queries: *mut dpiSubscrMessageQuery, num: u32) -> Vec<Query<'a>> {
    let mut vec = Vec::with_capacity(num as usize);
    for i in 0..(num as isize) {
        vec.push(Query::from_dpi(&*queries.offset(i)));
    }
    vec
}

#[derive(Debug)]
pub struct Query<'a> {
    id: u64,
    operation: OpCode,
    tables: Vec<Table<'a>>,
}

impl<'a> Query<'a> {
    pub(crate) unsafe fn from_dpi(query: &dpiSubscrMessageQuery) -> Query {
        let mut tables = Vec::with_capacity(query.numTables as usize);
        for i in 0..(query.numTables as isize) {
            tables.push(Table::from_dpi(&*query.tables.offset(i)));
        }
        Query {
            id: query.id,
            operation: OpCode::from_bits_truncate(query.operation),
            tables: tables,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn operation(&self) -> &OpCode {
        &self.operation
    }

    pub fn tables(&self) -> &[Table] {
        &self.tables
    }
}

#[derive(Debug)]
pub struct Table<'a> {
    operation: OpCode,
    name: Cow<'a, str>,
    rows: Vec<Row<'a>>,
}

impl<'a> Table<'a> {
    pub(crate) unsafe fn from_dpi(table: &dpiSubscrMessageTable) -> Table {
        let mut rows = Vec::with_capacity(table.numRows as usize);
        for i in 0..(table.numRows as isize) {
            rows.push(Row::from_dpi(&*table.rows.offset(i)));
        }
        Table {
            operation: OpCode::from_bits_truncate(table.operation),
            name: to_cow_str(table.name, table.nameLength),
            rows: rows,
        }
    }

    pub fn operation(&self) -> &OpCode {
        &self.operation
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    operation: OpCode,
    rowid: Cow<'a, str>,
}

impl<'a> Row<'a> {
    unsafe fn from_dpi(row: &dpiSubscrMessageRow) -> Row {
        Row {
            operation: OpCode::from_bits_truncate(row.operation),
            rowid: to_cow_str(row.rowid, row.rowidLength),
        }
    }

    pub fn operation(&self) -> &OpCode {
        &self.operation
    }

    pub fn rowid(&self) -> &str {
        &self.rowid
    }
}
