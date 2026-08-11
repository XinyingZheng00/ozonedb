<!--
Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.

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

## Quick Start

```
bash ${OZONEDB_HOME}/bench/scripts/build.sh
./bin/ycsb load ozonedb -s -P workloads/workloada (-p property)
```

`build.sh` produces `libOzoneDB.so` and `libozonedb.so` in `${OZONEDB_HOME}/build` and packages
this binding. There is nothing to install: `jni.OzoneDBJNI` (a source file in this module) loads
the shim from `$OZONEDB_HOME/build`, and the shim's `$ORIGIN` rpath finds `libOzoneDB.so` beside
it. Override the path with `-Dozonedb.native.lib=/abs/path/libozonedb.so` if you need to.

## Running YCSB against the CorfuDB backend

OzoneDB's CorfuDB backend reuses this same binding — the Java side is
unchanged. Switching backends is purely a matter of (a) pointing `shared_config`
at a config file whose `"backend"` field is `"corfu"`, and (b) making sure
`corfu-bridge-1.0-all.jar` is on the YCSB JVM's classpath so the native
`CorfuDBStorage` can `FindClass("site/ycsb/db/corfu/CorfuBridge")`.

### Prerequisites

1. Build OzoneDB with the Corfu backend enabled:
   ```
   cmake -B build -DOZONEDB_ENABLE_CORFU=ON
   cmake --build build --target OzoneDB ozonedb_jni corfu-bridge
   ```
   This produces `libOzoneDB.so`, `libozonedb.so`, and
   `${OZONEDB_HOME}/ozonedb-jni-maven/corfu-bridge/target/corfu-bridge-1.0-all.jar`.
2. Start a Corfu server somewhere reachable, e.g.
   ```
   corfu_server -m -s 9000
   ```
3. Make sure `JAVA_HOME` points at the same JDK that was used to build
   `corfu-bridge-1.0-all.jar` (currently JDK 25) and that
   `$JAVA_HOME/lib/server` is on `LD_LIBRARY_PATH` so YCSB can load libjvm.

### Via the bench scripts (recommended)

The local YCSB harness gained a new `db_name` — `ozonedb-corfu` — that
generates a corfu config file at runtime and passes the bridge jar through
YCSB's `-cp` flag. Wire it up in `bench/scripts/config/ycsb.yaml`:

```yaml
local:
  load:
    db_name:
      - "ozonedb-corfu"
    ...
  run:
    db_name:
      - "ozonedb-corfu"
    ...

corfu:
  endpoint: "127.0.0.1:9000"
  stream_name: "ozonedb-ycsb"
  jvm_opts: "-Xmx2g --add-opens=java.base/java.util=ALL-UNNAMED"
```

Then run as usual:

```
python3 bench/scripts/local/load_local_ycsb.py
python3 bench/scripts/local/run_local_ycsb.py
```

The generator writes `src/config/corfu/shared_config_rocksdb.json` from
`shared_config_rocksdb_base.json`, filling in the per-run `db_path` and the
absolute path to `corfu-bridge-1.0-all.jar`.

### Manual invocation

If you want to bypass the wrapper scripts:

```
cd ${OZONEDB_HOME}/ycsb
./bin/ycsb load ozonedb -s -P workloads/workloada \
    -p shared_config=${OZONEDB_HOME}/src/config/corfu/shared_config_rocksdb.json \
    -cp ${OZONEDB_HOME}/ozonedb-jni-maven/corfu-bridge/target/corfu-bridge-1.0-all.jar
```

Edit the config JSON beforehand so `corfu_jar_path`, `corfu_endpoint`, and
`db_path` match your environment.

### Troubleshooting

- `NoClassDefFoundError: site/ycsb/db/corfu/CorfuBridge` — the bridge jar is
  not on YCSB's classpath. Confirm the `-cp` arg (or re-run through the bench
  script) and that the jar exists at the configured path.
- `UnsupportedClassVersionError` — the JDK running YCSB is older than the one
  used to compile CorfuDB. Point `JAVA_HOME` at the build JDK and re-export
  `LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$LD_LIBRARY_PATH`.
- Handshake failures / connection refused — Corfu's default bind is IPv6; use
  `127.0.0.1:9000` (or `[::1]:9000`) as the endpoint rather than `localhost`.
