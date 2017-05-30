# A Rust Driver for CrateDB

[![Build Status](https://travis-ci.org/celaus/rust-cratedb.svg?branch=master)](https://travis-ci.org/celaus/rust-cratedb)
[![Crates.io](https://img.shields.io/crates/v/cratedb.svg)](https://crates.io/crates/cratedb)

CrateDB is a distributed SQL database by [Crate.io](https://crate.io), to which this driver provides access to.

## Quick Start

_The `None::<Box<NoParams>>` is required to tell the compiler about the type
the box would actually have. NoParams is an empty struct_ 😁 _if there's a better
solution, please open an issue_

```rust
extern crate cratedb;

use cratedb::{Cluster, NoParams};
use cratedb::sql::QueryRunner; // SQL query trait
use cratedb::blob::BlobContainer;  // BLOB container trait
use cratedb::row::ByIndex;
use std::iter;
use std::io::Cursor;

fn main() {
    // default URL for a local CrateDB instance
    let nodes = "http://localhost:4200/";

    // create a cluster
    let c: Cluster = Cluster::from_string(nodes).unwrap();

    // a simple query
    let stmt = "select hostname, name from sys.nodes";
    println!("Running: {}", stmt);
    let (elapsed, rows) = c.query(stmt, None::<Box<NoParams>>).unwrap();

    for r in rows {
      // cast and retrieve the values
      let hostname = r.as_string(0).unwrap();
      let nodename = r.as_string(1).unwrap();
        println!("hostname: {}, name: {}", hostname , nodename);
    }
    println!("The query took {} ms", elapsed);

    // DDL statements
    let (elapsed, rows) = c.query("create table a(a string)", None::<Box<NoParams>>).unwrap();

    // parameterized DML statements
    let p = Box::new(vec!(1234));
    let (elapsed, rows)  = c.query("insert into a(a) values (?)", Some(p)).unwrap();

    let bulk = vec!(["a"],["b"],["c"],["d"],["e"],["f"],["g"],["h"],["i"]);

    // parameterized bulk DML statements
    let stmt = "insert into a(a) values (?)";
    println!("Running: {}", stmt);
    let (elapsed, results)  = c.bulk_query(stmt, Box::new(bulk.clone())).unwrap();
    for r in results {
        println!("Inserted {} rows", r);
    }
    println!("The query took {} ms", elapsed);

    // drop this table
    let _  = c.query("drop table a", None::<Box<NoParams>>);

    let _ = c.query("create blob table b", None::<Box<NoParams>>)
        .unwrap();

    let myblob: Vec<u8> = iter::repeat(0xA).take(1024).collect();
    let r = c.put("b", &mut Cursor::new(&myblob)).unwrap();
    println!("Uploaded BLOB: {:?}", r);

    let mut actual = c.get(&r).unwrap();
    let mut buffer: Vec<u8> = vec![];
    let _ = actual.read_to_end(&mut buffer);
    assert_eq!(myblob, buffer);
    let _ = c.delete(r);

    let _ = c.query("drop blob table b", None::<Box<NoParams>>).unwrap();
}

```

**Output:**
```shell
Running: select hostname, name from sys.nodes
hostname: x5ff, name: Höllwand
The query took 1.322622 ms
Running: insert into a(a) values (?)
Inserted 1 rows
Inserted 1 rows
Inserted 1 rows
Inserted 1 rows
Inserted 1 rows
Inserted 1 rows
Inserted 1 rows
Inserted 1 rows
Inserted 1 rows
The query took 33.12071 ms
Uploaded BLOB: BlobRef { sha1: [143, 198, 224, 5, 9, 204, 175, 189, 111, 81, 168, 87, 152, 164, 23, 151, 240, 96, 249, 190], table: "b" }
```

# License

This project is developed under the [Apache 2.0](LICENSE) license.
