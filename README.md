# OzoneDB

A research LSM-tree key-value store whose entire durable state lives behind an append-only
"shared log" storage abstraction. Multiple independent writer *processes* — possibly on
different machines — coordinate through the log itself; there is no server. It builds as a
shared library (`libOzoneDB.so`) and is driven through a JNI binding from a YCSB client.

The build targets Ubuntu bench nodes. It is not set up to build on macOS.

## 1. Set up the machine

One idempotent, role-based script provisions every kind of node. Re-running it is cheap and
safe — it never duplicates shell config or reinstalls what is already there.

```bash
bash bench/scripts/setup.sh --role client         # the bench / YCSB machine
bash bench/scripts/setup.sh --role corfu-server   # the shared-log node
bash bench/scripts/setup.sh --role minio          # SSTable object store
```

No sourcing required. The script writes `~/.ozonedb.env` (with `OZONEDB_HOME`, `JAVA_HOME` and
`PATH`) and wires a marker-guarded include into `~/.profile` and `~/.bashrc`. Open a new shell
afterwards, or run `. ~/.ozonedb.env`.

The **client** role installs apt dependencies and a JDK, installs the Python requirements, creates
`/tank`, installs the CorfuDB runtime into `~/.m2` (which `corfu-bridge` needs
— it declares no `<repositories>`, so the dependency resolves only from a local install), then runs
`bench/scripts/build.sh`. Pass `--no-build` or `--no-corfu-runtime` to skip either.

For local experiments backed by ZFS, create the pool by naming the device — this no longer
partitions anything for you:

```bash
bash bench/scripts/setup_zfs.sh --list              # show candidate devices
bash bench/scripts/setup_zfs.sh --device /dev/sda6
```

`setup_node.sh` and `setup_corfu.sh` remain as thin wrappers around `setup.sh`.

### JDK versions

Three constraints conflict, so there are two knobs. YCSB compiles with `<source>1.8</source>`;
`corfu-bridge` targets Java 11, so anything running it needs JDK ≥ 11; CorfuDB itself wants a much
newer JDK. `--jdk` (default 17) sets the node's persistent `JAVA_HOME`; `--corfu-jdk` (default 25)
is used only to build CorfuDB. Both fall back through a candidate list when the exact apt package
is unavailable on the release.

## 2. Build

```bash
cmake -B build                          && cmake --build build -j$(nproc)  # local FS + Azure + S3
cmake -B build -DOZONEDB_ENABLE_CORFU=ON && cmake --build build -j$(nproc) # adds JNI + CorfuDB
```

No environment needs to be exported first: the vcpkg toolchain resolves from an explicit
`CMAKE_TOOLCHAIN_FILE`, else `$VCPKG_ROOT`, else the in-repo `vcpkg/` tree (which is vendored
— ~11.7k committed files — not a submodule, so a clone or rsync already has it).

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

### Pushing code to the nodes

Two Ansible playbooks, both fanning out to every host in `cloudlab.hosts`
concurrently. Hosts come from `ycsb.yaml`, so there is no second list to maintain.

```bash
cd bench/ansible
ansible-playbook bootstrap.yml             # fresh node: push tree (incl. vcpkg) + run setup.sh
ansible-playbook sync.yml -e build=true    # thereafter: push changes + rebuild
```

`bootstrap.yml` needs no `git clone` and no credentials on the node — `vcpkg` is
vendored rather than a submodule, so an rsync of the working tree is everything
the build needs. See `bench/ansible/README.md`.

## Tests

```bash
cd build && ./runUnitTests                          # all
./runUnitTests --gtest_filter='StorageTest.*'       # one suite
```

Run from `build/` — tests reference configs by relative path. `/tank` must exist and be writable.
See `CLAUDE.md` for which suites need credentials or a live Corfu endpoint.
