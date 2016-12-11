# A Rust Driver for CrateDB

CrateDB is a distributed SQL database by [Crate.io](https://crate.io), to which
this driver provides access to.



## Quick Start

```rust
extern crate cratedb;

use cratedb::Cluster;
use cratedb::row::ByIndex;

fn main() {
    let nodes = "https://play.crate.io".to_owned();

    let mut c:Cluster = Cluster::from_string(nodes).unwrap();
    let (t, rows) = c.query("select hostname, name from sys.nodes", None).unwrap();
    for r in rows {
        println!("hostname: {}, name: {}", r.as_string(0).unwrap(), r.as_string(1).unwrap());
    }
    let (t, rows)  = c.query("create table a(a string)", None).unwrap();
    println!("time {}, rows affected: ", t);
    let p = Box::new(vec!(1234));
    let _  = c.query("insert into a(a) values (?)", Some(p));
    let bulk = vec!(["a"],["b"],["c"],["d"],["e"],["f"],["g"],["h"],["i"]);
    let _  = c.bulk_query("insert into a(a) values (?)", Box::new(bulk.clone()));
    for r in  c.bulk_query("select * from a where a = ?", Box::new(bulk)) {
        println!("val: lalala");
    }

    let _ = c.query("refresh table a", None);
    let (duration, x) = c.query("select * from a", None).unwrap();
    for r in x {
        println!("val: {}", r.as_string(0).unwrap());
    }
    println!("The query took {} ms", duration);

    let _  = c.query("drop table a", None);
}
```

# License

This project is developed under the [Apache 2.0](LICENSE) license.
