<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# Apache Cassandra CQL binding

The upstream YCSB `cassandra` binding (DataStax Java driver 3.x, CQL),
vendored into the OzoneDB tree as the server-based baseline for the
consistency/throughput comparison. Two knobs were added on top of upstream.

## Schema

`bench/scripts/cassandra_ctl.sh schema` creates this on the server node.
For keyspace `ycsb`, table `usertable`:

    create keyspace ycsb
        WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1 };
    USE ycsb;
    create table usertable (
        y_id varchar primary key,
        field0 varchar, ... field9 varchar);

## Properties

Upstream:

- `hosts` (**required**) -- comma-separated contact points.
- `port` -- CQL port, default `9042`.
- `cassandra.keyspace` -- default `ycsb`.
- `cassandra.username`, `cassandra.password` -- optional.
- `cassandra.readconsistencylevel`, `cassandra.writeconsistencylevel` --
  default `QUORUM`. `SERIAL` on the read side gives a linearizable read.
- `cassandra.maxconnections`, `cassandra.coreconnections`,
  `cassandra.connecttimeoutmillis`, `cassandra.readtimeoutmillis`,
  `cassandra.useSSL`, `cassandra.tracing`.

Added here:

- `cassandra.lwt` -- default `false`. When `true`, every write is a
  lightweight transaction: `INSERT ... IF NOT EXISTS`,
  `UPDATE ... IF EXISTS`, `DELETE ... IF EXISTS`. Each one is a Paxos round.
  A write whose condition fails is counted and logged at cleanup, and
  reported as OK: it paid the full cost, which is what the benchmark measures.
- `cassandra.serialconsistencylevel` -- default `SERIAL`. The serial
  consistency level attached to those conditional writes.

The bench runners map `cassandra.consistency` in `ycsb.yaml` (or
`--cassandra_consistency`) onto these properties:

| mode     | read   | write  | lwt   |
|----------|--------|--------|-------|
| `one`    | ONE    | ONE    | false |
| `quorum` | QUORUM | QUORUM | false |
| `serial` | SERIAL | QUORUM | true  |

On a single node with replication factor 1, `one` and `quorum` cost the
same. `serial` is different on any topology: the Paxos rounds and the
serial read phase run even with one replica.
