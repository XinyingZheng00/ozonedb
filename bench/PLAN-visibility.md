# Plan: YCSB-shaped visibility benchmarks (branch `visibility`)

**Status (2026-08-20):** phase 1 implemented and validated. cr-sqlite
(laptop, 50 ms sync, rate 20): 94/94 first reads missed (100%), visibility
p50 35.2 ms, P[stale] = 0 past 100 ms. ozonedb (amd121/105/123 cluster,
15 s runs): default engine 1/282 missed (0.4%) at rate 20 and 2/1269 (0.2%)
at rate 100, visibility p50 ≈ 2–2.7 ms; `linearizable_reads` **0/280 and
0/1257 missed** at ~0.2–0.8 ms/get premium (p50 3.5 / 2.1 ms), 0 timeouts
everywhere. Figure: crsqlite repo `plot/out/visibility_rate20.pdf`.

**Phase 2 (cross-node) validated 2026-08-20:** writer amd121, reader
amd105, TCP notify. Default engine 1/281 missed (0.4%), visibility p50
2.6 ms (referenced to reader-side notify receipt — writer/reader
CLOCK_MONOTONIC domains are incomparable, and the miss count needs no
clocks at all); `linearizable_reads` **0/279 missed**, p50 3.0 ms, 0
timeouts. Cross-node adds no artifacts vs same-host. Driver:
`bash run_visibility_cross_node.sh [--linearizable] --rate 20 --duration 15`.
**Sweeps validated 2026-08-20 (matrix rows 3-4 done — plan complete):**
ozonedb rates 5/20/100/300 (achieved tops out ≈211/s: rate = 1/(period +
put latency)): `linearizable_reads` **0 misses at every rate** (0/73,
0/280, 0/1257, 0/3188) — the zero is load-invariant; default 1.4%/0.4%/
0.2%/0.2%. cr-sqlite sync-interval sweep 10/50/200 ms at rate 20:
visibility p50 7.7/25.8/104 ms ≈ interval/2 (log-log linear), miss rate
~100% throughout — staleness is structural, set by the config knob.
Figure: crsqlite repo `plot/out/visibility_sweep.pdf`
(`plot_consistency.py visibility-sweep <dirs...>`).

**Writer-scaling sweep 2026-08-21 (all-cluster, both engines on amd121):**
W ∈ {2,4,8,16} writers at 20/s each, 15 s, disjoint key ranges, one reader.
ozonedb `linearizable_reads` (one-fence): **0 misses at every W** (0/567,
0/1128, 0/2260, 0/4456), visibility p50 flat 2.3/4.3/3.5/5.6 ms — writers
share one log, so the reader's fence cost is writer-count-independent
(p99 grows to 390 ms at W=16: single reader polling 320 keys/s, tail is
reader-side). cr-sqlite-syncread (fenced, 50 ms interval, the other
0-miss config): p50 4.5/5.1/11.4/**82 ms**, p99 1.29 **s** at W=16 — the
/barrier pull round contacts all W peers, so fenced reads degrade with
the mesh. cr-sqlite async 10/50/200 ms: ~100% first-read misses at every
W (structural), and W=16 breaks even the 10 ms interval (p50 31.7 ms,
p90 279 ms, p99 971 ms). Figure (CDF-only, 2×2 facets by W): crsqlite
repo `plot/out/visibility_scaling.pdf`
(`plot_consistency.py visibility-scaling <dirs...>`; scaling runs are the
dirs whose summary has `writers` ≥ 2). Multi-writer plumbing:
`consistency.py check-visibility --writers N` (N insert-probes,
`--key-base` = w·10⁷, per-writer notify files, orchestrator-touched done
file); crsqlite `bench.py check-visibility --num-writers W` (writers on
replicas 0..W-1, reader on replica W).

**Rate sweep rerun 2026-08-21 after the one-fence strict-read optimization
(engine commit 2bd77fbd: one `Storage::sync` fence per get + thread-local
token, replacing 4–5 sequencer round-trips):** `linearizable_reads` still
**0 misses at every rate** (0/73, 0/279, 0/1255, 0/3268), 0 timeouts;
default 1.4%/0.4%/0.3%/0.2% — both unchanged, as expected. The strict
latency premium is gone: strict visibility p50 2.21/1.98/2.02/2.57 ms vs
default 3.34/2.82/1.77/2.80 ms — strict is now at or below default at 3
of 4 rates, where the 2026-08-20 multi-fence run had strict p50 3.5 ms vs
default ≈2.6 ms at rate 20. Harness note: this rerun exposed that the
consistency wrappers restarted corfu on a COPY of /mnt/corfu/load; once
the YCSB face-off load populated that dir (2026-08-20 14:17), every
fresh-stream openDB hung on the global-tail fence (tailAddress is
runtime-wide; the tailer only advances past its own stream's entries).
Both wrappers now start corfu on an empty /mnt/corfu/run_consistency.

## Context and goal

VLDB reviews ask for an empirical OzoneKV vs cr-sqlite comparison (R3-D3),
multi-node evaluation (R3-O5), and clearer experiments. We are NOT claiming
CAS in the paper, so the CAS prototype stays parked on branch `cas` and is
not part of the evaluation. Instead: two benchmarks that measure what the
paper does claim — fresh cross-writer reads — using YCSB-shaped workloads
and zero engine changes.

- **B1 — visibility latency** ("retry until found"): a writer inserts new
  keys; a reader polls each key until it appears; report the distribution
  of (first successful read − write ack). This is *t-visibility* in the
  PBS sense (Bailis et al., Probabilistically Bounded Staleness, VLDB'12).
- **B2 — fresh-read miss rate** ("count not found"): for each acked key,
  the reader's *first* get either finds it or doesn't; report the miss
  fraction. This is PBS's *probability of staleness*. Bucketing misses by
  the get's age (time since ack) yields the full P[stale] vs age curve.

Both come from ONE run: B2 is simply "did attempt #1 find the key", B1 is
"when did some attempt find it".

Expected headline (based on the probe-staleness results): ozonedb
`linearizable_reads` → **0 misses** and visibility ≈ the poll quantum;
ozonedb default → few-% misses, visibility p50 ≈ 1–2 ms (apply lag);
cr-sqlite → large miss fraction and visibility ≈ its sync interval.

## Two design decisions that carry the correctness argument

1. **Insert-only, brand-new keys.** For a key that never existed, "not
   found after the writer's ack" is unambiguous staleness — there is no
   older value to fall back on, no value comparison, no overlap-semantics
   argument. Keys come from the YCSB keyspace
   (`CoreWorkload.buildKeyName(i, 1, false)`, hashed order) with
   YCSB-sized values (default 1000 B), on a fresh Corfu stream / fresh
   cr-sqlite databases so the keyspace starts empty.

2. **Ack → notify → read gives clock-free happens-before.** The writer
   tells the reader about each key only *after* put() returned; the reader
   issues the first get only *after* receiving the notification. Any miss
   is therefore a genuine violation of read-latest, with no clock
   comparison involved — which makes the miss count valid across nodes
   (phase 2), not just on one host. Same-host runs additionally get exact
   latency from the shared CLOCK_MONOTONIC (System.nanoTime), as in the
   existing probe experiments.

## Components

### ConsistencyProbe.java — two new modes

- **`insert-probe`** (writer): for `--duration-s` at `--rate`/s, put key
  `buildKeyName(i, 1, false)` with a `--value-size` random value (first
  8 bytes = big-endian i, for debugging). After each put returns, record
  `(i, t_ack_ns)` in `inserts.csv` and append the line `i,t_ack_ns\n` to
  `--notify-file`, flushing per line. Reuses the ready/go barrier, openDB
  watchdog, and `System.exit(0)` discipline.
- **`visibility-probe`** (reader): single thread, single DB instance
  (the JNI layer's `db_instance` is thread_local, so multi-threaded
  readers would open N corfu clients — avoid). Loop every `--poll-ms`
  (default 1):
  1. read any new lines from `--notify-file` → add to a pending set with
     `t_notify_ns`;
  2. for each pending key issue one `get`; on the first attempt record
     `found_first` and `t_first_ns`; when found, record `t_found_ns` and
     `attempts` and retire the key; retire as `timeout` after
     `--key-timeout-s` (default 30).
  Ends when the writer's `--done-file` appears and pending drains. Emits
  `visibility.csv`:
  `idx,t_ack_ns,t_notify_ns,t_first_ns,found_first,t_found_ns,attempts`
  (t_found_ns = -1 for timeouts). The pending-set loop avoids
  head-of-line blocking: one slow key (cr-sqlite, ~50 ms) never delays
  checks of later keys.

### consistency.py — new subcommand `check-visibility`

- Flags: `--rate` (default 20), `--duration` (default 15), `--value-size`
  (1000), `--poll-ms` (1), `--key-timeout-s` (30); inherits the top-level
  `--linearizable` / `--rebuild`. Fresh stream `{stream}-vis-{tag}`,
  per-writer configs via `_make_configs`, `_kill_stale_probes`, barrier
  release, `timeout` guards — all as in `probe-staleness`.
- Analysis (from `visibility.csv`):
  - `misses_first_read`, `miss_first_read_fraction` (B2),
  - `visibility_ms` p50/p90/p95/p99/max/mean of `t_found − t_ack` (B1),
  - `first_check_age_ms` p50/p99 of `t_first − t_ack` (honesty metric:
    how old the key already was at first check),
  - `notify_lag_ms` p50 of `t_notify − t_ack` (channel overhead, ~µs),
  - `timeouts` (must be 0 for ozonedb; cr-sqlite needs
    key-timeout ≫ sync interval),
  - `pbs`: list of `{age_ms_le, p_stale}` buckets from all attempts,
  - the usual contract: `engine` (`ozonedb-corfu[-linearizable]`),
    `linearizable_reads`, `sync_interval_ms: 0`, `rate_per_s`, `stream`.
- Verdict line: linearizable runs assert `misses_first_read == 0`.

### cr-sqlite mirror (crsqlite repo)

`bench.py check-visibility`: writer process INSERTs new rows into the crr
table on replica A; reader polls replica B, same notify-file protocol,
same `visibility.csv` / `summary.json` shape with
`engine: cr-sqlite`, `sync_interval_ms` set. Sweep `--interval-ms`.

### Plots (crsqlite repo, plot_consistency.py)

New `visibility <dirs...>` subcommand: (a) visibility-latency CDF per
engine, (b) miss-fraction bar chart, (c) PBS curve (P[stale] vs age).
Labels from the summary `engine` field, as with the existing plots.

## Validation matrix (when the cluster is back)

| # | run | expectation |
|---|-----|-------------|
| 1 | ozonedb default, rate 20, 15 s, same-host | miss ≈ few %, vis p50 1–2 ms, 0 timeouts |
| 2 | ozonedb `--linearizable` | **0 misses**, vis ≈ poll quantum + fence (~1–2 ms) |
| 3 | cr-sqlite, 50 ms sync | miss large, vis p50 ≈ 25–50 ms |
| 4 | rate sweep 5/20/100 (both engines); cr-sqlite interval sweep 10/50/200 ms | miss/vis vs load and vs interval curves |
| 5 | phase 2 cross-node repeats of 1–3 | miss counts unchanged (clock-free) |

Driver: `bash run_consistency_with_corfu.sh [--linearizable]
check-visibility --rate 20 --duration 15` (wrapper unchanged for v1 —
same-host, laptop-driven corfu restart stays mandatory).

## Phase 2 — cross-node reader (R3-O5)

Writer on client0, reader on client1; notify over TCP (reader listens on
the LAN address, writer connects; probe flags `--notify-listen` /
`--notify-connect` replacing the file). Orchestration is laptop-driven
like the corfu restart: `run_consistency_with_corfu.sh` starts the reader
on client1 via ssh (client nodes cannot ssh each other — only the laptop
holds the key). Miss counting stays exact (happens-before through the
socket); latency gains one ~50 µs LAN hop, negligible at ms/50 ms scales
— state both in the writeup.

## Risks / notes

- Poll interval bounds B1 resolution — report `poll_ms` in the summary
  and say so in the paper.
- Reader throughput: one get per pending key per tick; with strict gets
  ~1 ms each, keep rate × expected-lag ≪ 1000/ms-per-get (fine at the
  planned rates; log a warning if the pending set exceeds ~100).
- Value size affects apply lag — sweep only if time permits; default
  1000 B matches YCSB.
- All the corfu operational rules apply: fresh restarted server per run,
  orphan probe kill, fresh stream, JDK 25.

## Non-goals

- CAS / atomic RMW (parked on branch `cas`; rebuttal material only).
- YCSB throughput comparison (separate work, same cluster).
- Engine changes of any kind — this branch is bench-only.
