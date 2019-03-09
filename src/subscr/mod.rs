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

use std::fmt;
use std::os::raw::c_void;
use std::ptr;
use std::time::Duration;
use try_from::TryInto;

pub mod aq;
pub mod cqn;

use binding::*;
use private;
use to_odpi_str;
use Connection;
use Result;

pub(crate) enum Callback {
    CQN(cqn::Callback),
    AQ(aq::Callback),
}

impl fmt::Debug for Callback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Callback::CQN(ref cb) => write!(f, "CQN({:p})", cb),
            &Callback::AQ(ref cb) => write!(f, "AQ({:p})", cb),
        }
    }
}

unsafe extern "C" fn callback_wrapper(context: *mut c_void, msg: *mut dpiSubscrMessage) {
    let callback = &mut *(context as *mut Callback);
    match callback {
        &mut Callback::CQN(ref mut cqn_callback) => {
            let event = cqn::Event::new(&*msg);
            cqn_callback(event);
        }
        &mut Callback::AQ(ref mut aq_callback) => {
            let event = aq::Event::new(&*msg);
            aq_callback(event);
        }
    }
}

pub trait Protocol: private::Sealed {
    fn init_form(self, form: &mut SubscriptionForm);
}

bitflags! {
    pub struct QOS: dpiSubscrQOS {
        const RELIABLE = DPI_SUBSCR_QOS_RELIABLE;
        const DEREG_NFY = DPI_SUBSCR_QOS_DEREG_NFY;
        const ROWIDS = DPI_SUBSCR_QOS_ROWIDS;
        const QUERY = DPI_SUBSCR_QOS_QUERY;
        const BEST_EFFORT = DPI_SUBSCR_QOS_BEST_EFFORT;
    }
}

bitflags! {
    pub struct OpCode: dpiOpCode {
        const ALL_OPS = DPI_OPCODE_ALL_OPS;
        const ALL_ROWS = DPI_OPCODE_ALL_ROWS;
        const INSERT = DPI_OPCODE_INSERT;
        const UPDATE = DPI_OPCODE_UPDATE;
        const DELETE = DPI_OPCODE_DELETE;
        const ALTER = DPI_OPCODE_ALTER;
        const DROP = DPI_OPCODE_DROP;
        const UNKNOWN = DPI_OPCODE_UNKNOWN;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum GroupingClass {
    Time(Duration),
}

#[derive(Clone, Debug, PartialEq)]
pub enum GroupingType {
    Summary,
    Last,
}

#[derive(Debug)]
pub struct SubscriptionForm {
    pub(crate) namespace: dpiSubscrNamespace,
    pub(crate) protocol: dpiSubscrProtocol,
    pub(crate) callback: Option<Callback>,
    pub(crate) recipient: Option<String>,
    qos: QOS,
    op_code: OpCode,
    port: u32,
    timeout: Duration,
    name: String,
    ip_address: String,
    grouping_class: Option<GroupingClass>,
    grouping_type: Option<GroupingType>,
}

impl SubscriptionForm {
    fn default() -> SubscriptionForm {
        SubscriptionForm {
            namespace: 0,
            protocol: 0,
            callback: None,
            recipient: None,
            qos: QOS::empty(),
            op_code: OpCode::empty(),
            port: 0,
            timeout: Duration::new(0, 0),
            name: "".into(),
            ip_address: "".into(),
            grouping_class: None,
            grouping_type: None,
        }
    }

    pub fn new<P: Protocol>(protocol: P) -> SubscriptionForm {
        let mut form = SubscriptionForm::default();
        protocol.init_form(&mut form);
        form
    }

    pub fn qos(&mut self, qos: QOS) -> &mut SubscriptionForm {
        self.qos = qos;
        self
    }

    pub fn op_code(&mut self, op_code: OpCode) -> &mut SubscriptionForm {
        self.op_code = op_code;
        self
    }

    pub fn port(&mut self, port: u32) -> &mut SubscriptionForm {
        self.port = port;
        self
    }

    pub fn timeout(&mut self, dur: Duration) -> &mut SubscriptionForm {
        self.timeout = dur;
        self
    }

    pub fn name(&mut self, name: String) -> &mut SubscriptionForm {
        self.name = name;
        self
    }

    pub fn ip_address(&mut self, addr: String) -> &mut SubscriptionForm {
        self.ip_address = addr;
        self
    }

    pub fn grouping_class(&mut self, grouping_class: GroupingClass) -> &mut SubscriptionForm {
        self.grouping_class = Some(grouping_class);
        self
    }

    pub fn grouping_type(&mut self, grouping_type: GroupingType) -> &mut SubscriptionForm {
        self.grouping_type = Some(grouping_type);
        self
    }

    pub fn submit<'conn>(self, conn: &'conn Connection) -> Result<Subscription<'conn>> {
        let mut subscr = Subscription {
            conn: conn,
            handle: ptr::null_mut(),
            callback: ptr::null_mut(),
        };

        let mut params = conn.ctxt.subscr_create_params;
        params.subscrNamespace = self.namespace;
        params.protocol = self.protocol;

        if let Some(callback) = self.callback {
            subscr.callback = Box::into_raw(Box::new(callback));
            params.callback = Some(callback_wrapper);
            params.callbackContext = subscr.callback as *mut c_void;
        }
        if let Some(ref recipient) = self.recipient {
            let recipient = to_odpi_str(recipient);
            params.recipientName = recipient.ptr;
            params.recipientNameLength = recipient.len;
        }

        params.qos = self.qos.bits();
        params.operations = self.op_code.bits();
        params.portNumber = self.port;
        params.timeout = self.timeout.as_secs().try_into()?;
        let name = to_odpi_str(&self.name);
        params.name = name.ptr;
        params.nameLength = name.len;
        let addr = to_odpi_str(&self.ip_address);
        params.ipAddress = addr.ptr;
        params.ipAddressLength = addr.len;
        if let Some(class) = self.grouping_class {
            match class {
                GroupingClass::Time(dur) => {
                    params.groupingClass = DPI_SUBSCR_GROUPING_CLASS_TIME as dpiSubscrGroupingClass;
                    params.groupingValue = dur.as_secs().try_into()?;
                }
            }
        }
        if let Some(type_) = self.grouping_type {
            match type_ {
                GroupingType::Summary => {
                    params.groupingType = DPI_SUBSCR_GROUPING_TYPE_SUMMARY as dpiSubscrGroupingType;
                }
                GroupingType::Last => {
                    params.groupingType = DPI_SUBSCR_GROUPING_TYPE_LAST as dpiSubscrGroupingType;
                }
            }
        }
        chkerr!(
            conn.ctxt,
            dpiConn_subscribe(conn.handle, &mut params, &mut subscr.handle)
        );
        Ok(subscr)
    }
}

pub struct Subscription<'conn> {
    conn: &'conn Connection,
    handle: *mut dpiSubscr,
    callback: *mut Callback,
}

impl<'conn> Subscription<'conn> {
    pub fn set_query(&mut self, sql: &str) -> Result<u64> {
        let sql = to_odpi_str(sql);
        let mut handle = ptr::null_mut();
        chkerr!(
            self.conn.ctxt,
            dpiSubscr_prepareStmt(self.handle, sql.ptr, sql.len, &mut handle)
        );
        let mut num_cols = 0;
        chkerr!(
            self.conn.ctxt,
            dpiStmt_execute(handle, DPI_MODE_EXEC_DEFAULT, &mut num_cols),
            unsafe {
                dpiStmt_release(handle);
            }
        );
        let mut query_id = 0;
        chkerr!(
            self.conn.ctxt,
            dpiStmt_getSubscrQueryId(handle, &mut query_id),
            unsafe {
                dpiStmt_release(handle);
            }
        );
        unsafe {
            dpiStmt_release(handle);
        }
        Ok(query_id)
    }
}

impl<'conn> Drop for Subscription<'conn> {
    fn drop(&mut self) {
        unsafe {
            dpiSubscr_release(self.handle);
            if !self.callback.is_null() {
                // to drop callback
                Box::from_raw(self.callback);
            }
        }
    }
}
