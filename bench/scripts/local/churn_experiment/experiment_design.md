# Churn-tolerance experiment for OzoneDB — fixed-backlog completion-time

## Hypothesis

When OzoneDB faces a fixed compaction backlog, the append-based churn-recovery
protocol (Section 4.4 of the paper) completes it correctly under churn:

1. **Liveness** — every compaction task in the backlog (and every cascade
   compaction it triggers) eventually reaches a `TASK_COMPLETE` record.
2. **Correctness** — every key written before the experiment is still
   readable post-completion with the value last assigned to it, regardless of
   the per-compaction abort rate.
3. **Bounded scaling** — completion time decreases (or at worst stays flat) as
   the writer count `N` grows; completion time increases as the abort rate `R`
   grows, bounded by per-task `max_generation × T_d`.

### Success criteria

- Integrity pass rate = 100% across all conditions.
- Completion phase converges within `completion_timeout_s` for every condition.
- Completion time monotonically increases with `R` (within single-trial noise).

## Cluster setup

Single host. All writers are local OS processes that JNI-load
`libozonedb.so` and open the same on-disk DB directory. "Node churn" maps
to "compaction returns failure"; the protocol cannot tell the difference
between an aborted compaction and one whose owner crashed mid-flight.

| Parameter | Value | Notes |
| --- | --- | --- |
| Records | 50,000 × 1 KB | Loaded once with compaction disabled, then snapshot. |
| Writers `N` | varied ∈ {1, 2, 4, 8} (when `--vary writers`) or fixed at 4 (when `--vary abort`) | All read `OZONEDB_CHURN_ABORT_RATE` from env. |
| Abort rate `R` | varied ∈ {0%, 10%, 20%, 30%, 50%} (when `--vary abort`) or fixed at 0% (when `--vary writers`) | Per-compaction probability of returning early without writing `TASK_COMPLETE`. |
| Completion timeout | 600 s per trial | Hard cap on Phase B. |
| Quiescence grace | 8 s | Completion declared "done" when every task has a `COMPLETE` record AND `n_tasks_total` has been flat for 8 s. |
| Storage config | `log_file=1 MB`, `level_size=[16 MB, 256 MB]`, `level_file_size_limit=[32 MB, 256 MB]`, `max_level=2` | Last-level limit is large enough that last-layer compactions always merge into a single output, so the cascade converges. |
| `task_heartbeat_threshold` | 30 ticks ≈ 3 s `T_d` (patched) | See deviations. |

### Patches & rebuild (deviations from as-shipped code)

Six source-level changes from the as-shipped code. Four are real bug
fixes; two are experimental knobs.

1. **`task_heartbeat_threshold`** in
   `src/include/ozonedb/task_log_handler.h:61` from `10000000` → `30`.
   Required: as-shipped value caps timeout at ~11.6 days, so the
   dead-task path can't fire in any practical experiment window.

2. **Early `COMPACT_START` stdout marker** at the entry of
   `doCompactionWork` in `src/db/compaction.cpp`. Used to count
   compactions per trial; the original `Compacting...` line fires at the
   *end* of the function and so can't be used for instrumentation.

3. **Step 0 dead-task priority** in `pickCompaction`. Required: under
   continuous workload, Steps 1–3 always find live work and starve the
   dead-task path indefinitely.

4. **`OZONEDB_CHURN_ABORT_RATE` env knob** in `doCompactionWork`. With
   probability `R`, set the heartbeat thread's `aborted` flag and return
   `kFailure` immediately. The heartbeat thread checks the flag and exits
   without writing `TASK_COMPLETE`, leaving the task abandoned exactly as
   if the writer had crashed mid-compaction. No-op when env unset.

5. **`rollForwardTaskLog()` called from `pickCompaction`**. As-shipped,
   rollforward only ran inside `shouldWorkOnTask` — i.e., only when a
   writer was actively trying to claim a task. A quiescent writer (e.g.,
   one whose live-work view is empty) would never call rollforward, so
   the heartbeat-timeout check inside it never ran, so dead tasks were
   never discovered.

6. **Defensive `find()`-first in `rollforwardSingleOperationRecord`** at
   `src/db/metadata_log_handler.cpp:152-209`. Both the last-level COMPACT
   path (was crashing on `erase(end())`) and the non-last-level path
   (was crashing on `pop_front()` from an empty deque or popping the
   wrong element) are now guarded with `find()` + skip-with-warning when
   the input file isn't in the layout. Fixes a SIGSEGV in
   `__copy_move_backward_a1<...basic_string...>` that fired under
   high-cascade churn.

The shared library is rebuilt after any patch with the standard two-step:

```bash
cd $OZONEDB_HOME/build && cmake --build . -j
cp libOzoneDB.so $OZONEDB_HOME/ozonedb-jni-maven/native/src/main/cpp/lib/
cd $OZONEDB_HOME/ozonedb-jni-maven && mvn clean package
sudo cp jni/target/classes/libozonedb.so /usr/lib/
```

## Per-trial flow

1. **One-time setup (shared across all trials in the run)**: load 50 k
   records single-process with `compaction_policy=1` (skips
   `startCompactionWatcher`). Result: 39 sealed log files, 0 sstables,
   empty `task.log`. Snapshot saved at
   `/tank/test/churn_v4/snapshot_loaded_no_compaction/` for `cp`-ing into
   each trial.

2. **Per-trial DB**: `cp -r --reflink=auto` the snapshot to a per-trial
   working dir.

3. **Completion phase (measured)**: spawn `N` writers with
   `compaction_policy=0` (compaction enabled) and
   `OZONEDB_CHURN_ABORT_RATE=R`, all running YCSB workload-c
   (read-only). Their `CompactionWatcher`s discover the log backlog and
   start compacting; each successful log compaction triggers cascade
   work in level 1 / level 2. Orchestrator polls `task.log` once per
   second; declares quiescent when every task has at least one
   `COMPLETE` record AND `n_tasks_total` has been flat for 8 s. SIGTERM
   the writers (their `closeDB` joins the in-flight compaction).

4. **Integrity check**: single-process YCSB workload-c with
   `requestdistribution=sequential`, `operationcount=50000`. Reads every
   loaded key once. `integrity_pass` iff `Return=OK == 50000`.

5. **Tear down**: wipe the per-trial DB dir.

## Metrics (per trial, into `trials.csv`)

| Metric | Definition |
| --- | --- |
| `vary` | "writers" or "abort" — which parameter this trial varies. |
| `n_writers` | Writer count for this trial. |
| `abort_rate` | Configured `R` for this trial. |
| `completion_time_s` | Wallclock for Phase B (includes the 8 s grace tail). |
| `completed` | bool: did all tasks reach COMPLETE before the completion timeout? |
| `n_tasks_total` | Unique TaskIDs in `task.log` at end of completion. |
| `n_tasks_with_reassignment` | Tasks that needed at least one reclaim (`max_generation > 0`). |
| `max_generation_observed` | Highest gen any task reached (1 = aborted once and recovered). |
| `generation_distribution` | Full histogram, e.g. `0:23;1:5;2:1`. |
| `integrity_pass` | bool: did the post-completion sequential read return OK for all 50 k keys? |
| `notes` | Free-form annotations (timeouts, anomalies). |

## Reproduction

```bash
cd $OZONEDB_HOME
# (one-time) apply the six source patches and rebuild — see Patches above.
python3 bench/scripts/local/churn_experiment/churn_runner.py
python3 bench/scripts/local/churn_experiment/make_plots.py \
    bench/scripts/local/churn_experiment/results/v4_<timestamp>/trials.csv
```

CLI options on `churn_runner.py`:

- `--vary writers|abort|both` — which parameter to vary (default `both`).
- `--writers 1,2,4,8` — N values when `--vary=writers`.
- `--abort-rates 0.0,0.10,0.20,0.30,0.50` — R values when `--vary=abort`.
- `--fixed-r 0.0` — R held constant when `--vary=writers`.
- `--fixed-n 4` — N held constant when `--vary=abort`.
- `--rebuild-snapshot` — force re-running the priming load.
- `--record-count 50000` — dataset size.

## Output artifacts

```
churn_experiment/
├── experiment_design.md          ← this file
├── FINAL_REPORT.md               ← results + analysis
├── churn_runner.py               ← orchestrator
├── make_plots.py                 ← results.csv → PDFs
├── task_log_parser.py            ← shared task.log parser
├── churn_config.json             ← shared OzoneDB config
├── record_pb2.py                 ← generated protobuf (TaskRecord)
└── results/v4_<timestamp>/
    ├── trials.csv, run_config.json
    ├── per_trial/<trial_id>/{load,writer_*,verify}.log
    │                          task_log_timeline.json
    └── plots/
        ├── completion_time_vs_writers.pdf
        ├── completion_time_vs_abort.pdf
        ├── max_generation.pdf
        └── generation_dist.pdf
```
