# A Rust Driver for CrateDB

[![Build Status](https://travis-ci.org/celaus/rust-cratedb.svg?branch=master)](https://travis-ci.org/celaus/rust-cratedb)

CrateDB is a distributed SQL database by [Crate.io](https://crate.io), to which this driver provides access to.

## Quick Start

```rust
extern crate cratedb;

use cratedb::Cluster;
use cratedb::row::ByIndex;

fn main() {
    // default URL for a local CrateDB instance
    let nodes = "http://localhost:4200/".to_owned();

    // create a cluster
    let mut c: Cluster = Cluster::from_string(nodes).unwrap();

    // a simple query
    let stmt = "select hostname, name from sys.nodes";
    println!("Running: {}", stmt);
    let (elapsed, rows) = c.query(stmt, None).unwrap();

    for r in rows {
      // cast and retrieve the values
      let hostname = r.as_string(0).unwrap();
      let nodename = r.as_string(1).unwrap();
        println!("hostname: {}, name: {}", hostname , nodename);
    }
    println!("The query took {} ms", elapsed);

    // DDL statements
    let (elapsed, rows) = c.query("create table a(a string)", None).unwrap();

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
    let _  = c.query("drop table a", None);
}

```

# License

This project is developed under the [Apache 2.0](LICENSE) license.
