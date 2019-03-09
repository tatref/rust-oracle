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

use binding::*;
use error::error_from_dpi_error;
use private;
use std::borrow::Cow;
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
                form.callback = Some(super::Callback::AQ(cb));
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
    AQ,
    Deregister,
}

impl EventType {
    fn from_dpi(val: dpiEventType) -> Result<EventType> {
        Ok(match val {
            DPI_EVENT_AQ => EventType::AQ,
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
    queue_name: Cow<'a, str>,
    consumer_name: Cow<'a, str>,
    registered: bool,
}

impl<'a> Event<'a> {
    pub(crate) unsafe fn new(msg: &dpiSubscrMessage) -> Result<Event> {
        if msg.errorInfo.is_null() {
            Ok(Event {
                event_type: EventType::from_dpi(msg.eventType).unwrap(),
                queue_name: to_cow_str(msg.queueName, msg.queueNameLength),
                consumer_name: to_cow_str(msg.consumerName, msg.consumerNameLength),
                registered: msg.registered != 0,
            })
        } else {
            Err(error_from_dpi_error(&*msg.errorInfo))
        }
    }

    pub fn event_type(&self) -> &EventType {
        &self.event_type
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    pub fn registered(&self) -> bool {
        self.registered
    }
}
