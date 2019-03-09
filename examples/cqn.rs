// Rust-oracle - Rust binding for Oracle database
//
// URL: https://github.com/kubo/rust-oracle
//
//-----------------------------------------------------------------------------
// Copyright (c) 2019 Kubo Takehiro <kubo@jiubao.org>. All rights reserved.
// This program is free software: you can modify it and/or redistribute it
// under the terms of:
//
// (i)  the Universal Permissive License v 1.0 or at your option, any
//      later version (http://oss.oracle.com/licenses/upl); and/or
//
// (ii) the Apache License v 2.0. (http://www.apache.org/licenses/LICENSE-2.0)
//-----------------------------------------------------------------------------

extern crate getopts;
extern crate oracle;

use oracle::subscr::{cqn, SubscriptionForm, QOS};
use oracle::{ConnParam, Connection, Result};
use std::env;
use std::io::{self, BufRead, Write};
use std::process::exit;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

fn print_usage_and_exit(program: &str, opts: getopts::Options, exit_code: i32) -> ! {
    let brief = format!("Usage: {} [options] TABLE_NAME...", program);
    print!("{}", opts.usage(&brief));
    exit(exit_code);
}

fn parse_arguments() -> getopts::Matches {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = getopts::Options::new();
    opts.reqopt("u", "user", "user name (required)", "USERNAME");
    opts.reqopt("p", "password", "user password (required)", "PASSWORD");
    opts.optopt(
        "d",
        "database",
        "database name (connect string such as //host_name/service_name or a tns name)",
        "DATABASE",
    );
    opts.optflag("", "query", "query level");
    opts.optflag("", "rowids", "with rowid information");
    opts.optopt("t", "timeout", "timeout in seconds", "TIMEOUT");
    opts.optflag("h", "help", "print this help menu");
    match opts.parse(&args[1..]) {
        Ok(matches) => matches,
        Err(fail) => {
            println!("{}", fail.to_string());
            print_usage_and_exit(&program, opts, 1);
        }
    }
}

fn main() -> Result<()> {
    let matches = parse_arguments();
    let username = matches.opt_str("u").unwrap();
    let password = matches.opt_str("p").unwrap();
    let database = matches.opt_str("d").unwrap_or("".into());

    let conn = Connection::connect(&username, &password, &database, &[ConnParam::Events])?;

    let lock_cvar_pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair = lock_cvar_pair.clone();
    let protocol = cqn::Protocol::callback(move |result| {
        let &(ref lock, ref cvar) = &*pair;

        print_event(&result);
        if let Ok(event) = result {
            if event.event_type() == &cqn::EventType::Deregister {
                let mut stop = lock.lock().unwrap();
                *stop = true;
                cvar.notify_all();
            }
        }
    });
    let mut form = SubscriptionForm::new(protocol);
    let mut qos = QOS::empty();
    if matches.opt_present("query") {
        qos.insert(QOS::QUERY);
    }
    if matches.opt_present("rowids") {
        qos.insert(QOS::ROWIDS);
    }
    if !qos.is_empty() {
        form.qos(qos);
    }
    if let Some(secs) = matches.opt_get("timeout").unwrap() {
        form.timeout(Duration::from_secs(secs));
    }

    let mut subscr = form.submit(&conn)?;

    for table_name in matches.free {
        let sql = format!("select * from {}", table_name);
        let req_id = subscr.set_query(&sql)?;
        println!("Request ID of \"{}\" is {}", sql, req_id);
    }

    println!("*********************************");
    println!("   Hit enter to stop!");
    println!("*********************************");

    let pair = lock_cvar_pair.clone();
    thread::spawn(move || {
        let &(ref lock, ref cvar) = &*pair;
        io::stdout().flush().unwrap();
        let stdin = io::stdin();
        stdin.lock().lines().next();
        let mut stop = lock.lock().unwrap();
        *stop = true;
        cvar.notify_all();
    });

    let &(ref lock, ref cvar) = &*lock_cvar_pair;
    let mut stop = lock.lock().unwrap();
    while !*stop {
        stop = cvar.wait(stop).unwrap();
    }
    Ok(())
}

fn print_event(result: &Result<cqn::Event>) {
    match result {
        &Ok(ref event) => {
            println!("- {:?}:", event.event_type());
            if !event.database().is_empty() {
                println!("    database: {}", event.database());
            }
            print_tables(event.tables(), 4);
            print_queries(event.queries(), 4);
            if !event.transaction_id().is_empty() {
                println!("    transaction_id: {:?}", event.transaction_id());
            }
            println!("    registered: {}", event.registered());
        }
        &Err(ref err) => {
            println!("ERROR: {:?}", err);
        }
    }
}

fn print_queries(queries: &[cqn::Query], indent: usize) {
    if queries.is_empty() {
        return;
    }
    println!("{:indent$}queries:", "", indent = indent);
    for query in queries {
        println!("{:indent$}  - id: {}", "", query.id(), indent = indent);
        println!(
            "{:indent$}    operation: {:?}",
            "",
            query.operation(),
            indent = indent
        );
        print_tables(query.tables(), indent + 4);
    }
}

fn print_tables(tables: &[cqn::Table], indent: usize) {
    if tables.is_empty() {
        return;
    }
    println!("{:indent$}tables:", "", indent = indent);
    for table in tables {
        println!("{:indent$}  - name: {}", "", table.name(), indent = indent);
        println!(
            "{:indent$}    operation: {:?}",
            "",
            table.operation(),
            indent = indent
        );
        print_rows(table.rows(), indent + 4);
    }
}

fn print_rows(rows: &[cqn::Row], indent: usize) {
    if rows.is_empty() {
        return;
    }
    println!("{:indent$}rows:", "", indent = indent);
    for row in rows {
        println!("{:indent$}  - rowid: {}", "", row.rowid(), indent = indent);
        println!(
            "{:indent$}    operation: {:?}",
            "",
            row.operation(),
            indent = indent
        );
    }
}
