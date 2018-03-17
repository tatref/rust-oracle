// Rust-oracle - Rust binding for Oracle database
//
// URL: https://github.com/kubo/rust-oracle
//
// ------------------------------------------------------
//
// Copyright 2019 Kubo Takehiro <kubo@jiubao.org>
//
// Redistribution and use in source and binary forms, with or without modification, are
// permitted provided that the following conditions are met:
//
//    1. Redistributions of source code must retain the above copyright notice, this list of
//       conditions and the following disclaimer.
//
//    2. Redistributions in binary form must reproduce the above copyright notice, this list
//       of conditions and the following disclaimer in the documentation and/or other materials
//       provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHORS ''AS IS'' AND ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
// ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// The views and conclusions contained in the software and documentation are those of the
// authors and should not be interpreted as representing official policies, either expressed
// or implied, of the authors.

extern crate oracle;
mod common;
use oracle::{Connection, FetchMode, StmtParam};

#[test]
fn scroll() {
    let conn = common::connect().unwrap();
    scroll_sub(&conn, 1);
    scroll_sub(&conn, 9);
    scroll_sub(&conn, 10);
    scroll_sub(&conn, 11);
}

#[test]
fn iterate_scrollable_cursor() {
    let conn = common::connect().unwrap();

    let params = [
        StmtParam::Scrollable,
        StmtParam::FetchArraySize(3),
    ];

    let mut stmt = conn
        .prepare("select IntCol from TestStrings order by IntCol", &params)
        .unwrap();
    let rows = stmt.query_as::<i32>(&[]).unwrap();
    let mut idx = 0;
    for row_result in rows {
        idx += 1;
        assert_eq!(row_result.unwrap(), idx);
    }
    assert_eq!(idx, 10);
}

macro_rules! assert_result {
    ($rows:expr, $mode:expr, $expected_val:expr) => {
        match $rows.fetch($mode) {
            Ok(row) => assert_eq!(row, $expected_val, "expected IntCol value is {} but {}", $expected_val, row),
            Err(err) => panic!(format!("Error \"{}\" when fetching as {:?} and expected value is {}", err, $mode, $expected_val)),
        }
        let row_count = $rows.row_count().unwrap() as i32;
        assert_eq!(row_count, $expected_val, "expected row count is {} but {}", $expected_val, row_count);
    };
}

fn scroll_sub(conn: &Connection, fetch_array_size: u32) {
    let params = [
        StmtParam::Scrollable,
        StmtParam::FetchArraySize(fetch_array_size),
    ];

    let mut stmt = conn
        .prepare("select IntCol from TestStrings order by IntCol", &params)
        .unwrap();
    let mut rows = stmt.query_as::<i32>(&[]).unwrap();

    assert_eq!(rows.row_count().unwrap(), 0);
    for i in 1..11 {
        assert_result!(rows, FetchMode::Next, i);
    }
    assert_result!(rows, FetchMode::Absolute(3), 3);
    assert_result!(rows, FetchMode::First, 1);
    assert_result!(rows, FetchMode::Last, 10);
    assert_result!(rows, FetchMode::Relative(-1), 9);
    assert_result!(rows, FetchMode::Relative(-2), 7);
    assert_result!(rows, FetchMode::Relative(3), 10);

    let mut idx = 10;
    while idx > 1 {
        idx -= 1;
        assert_result!(rows, FetchMode::Prior, idx);
    }
}
