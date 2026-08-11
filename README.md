# OzoneDB

A research LSM-tree key-value store whose entire durable state lives behind an append-only
"shared log" storage abstraction. Multiple independent writer *processes* — possibly on
different machines — coordinate through the log itself; there is no server. It builds as a
shared library (`libOzoneDB.so`) and is driven through a JNI binding from a YCSB client.

The build targets Ubuntu bench nodes. It is not set up to build on macOS.

## 1. Set up the machine

`OZONEDB_HOME` must point at the repo root, so this must be **sourced**, not executed —
it appends `OZONEDB_HOME` and `JAVA_HOME` to your shell profile:

```bash
. bench/scripts/setup_node.sh
```

That pulls submodules, installs apt dependencies and a JDK, then builds the whole chain via
`bench/scripts/build.sh`.

For the CorfuDB shared-log backend, on a separate node:

```bash
bash bench/scripts/setup_corfu.sh
```

For local experiments backed by ZFS:

```bash
bash bench/scripts/setup_zfs.sh
```

## 2. Build

```bash
cmake -B build                          && cmake --build build -j$(nproc)  # local FS + Azure + S3
cmake -B build -DOZONEDB_ENABLE_CORFU=ON && cmake --build build -j$(nproc) # adds JNI + CorfuDB
```

No environment needs to be exported first: the vcpkg toolchain resolves from an explicit
`CMAKE_TOOLCHAIN_FILE`, else `$VCPKG_ROOT`, else the in-repo `vcpkg/` submodule.

With CMake 3.21 or newer you can use the presets instead — `cmake --preset default`,
`--preset corfu`, or `--preset profiling`.

After any C++ change that a YCSB run must see, rebuild the whole chain — otherwise YCSB keeps
running the previously installed library:

```bash
bash bench/scripts/build.sh
```

## 3. Run benchmarks

Everything is driven by `bench/scripts/config/ycsb.yaml` (record counts, workloads, thread and
writer counts, Corfu endpoint, S3/MinIO settings, CloudLab hosts).

```bash
# single machine, N parallel writer processes
python3 bench/scripts/local/load_local_ycsb_multiproc.py   # load phase
python3 bench/scripts/local/run_local_ycsb_multiproc.py    # run phase

# sweep workloads x writer counts, restarting Corfu between iterations
bash bench/scripts/local/run_ycsb_with_corfu.sh --workloads "a c" --writers-list "1 2 4"

# fan out across CloudLab hosts
python3 bench/scripts/local/run_multinode_ycsb.py
```

Results land in `bench/results/local` (gitignored). Plotting scripts are in `bench/scripts/plot/`.

## Tests

```bash
cd build && ./runUnitTests                          # all
./runUnitTests --gtest_filter='StorageTest.*'       # one suite
```

Run from `build/` — tests reference configs by relative path. `/tank` must exist and be writable.
See `CLAUDE.md` for which suites need credentials or a live Corfu endpoint.
