# Results: strict-consistency throughput frontier (run 1)

**Date:** 2026-08-25. **Cluster:** CloudLab amd217 (log/store/cassandra) + 7 clients.
**Dataset:** 1,000,000 x 1KB records. **Replication:** factor 1 on both systems.
**Trial:** 1. **Consistency:** OzoneDB `--linearizable`, Cassandra `--consistency serial`
(SERIAL reads + LWT writes). **Metric:** steady-state ops/sec, mean of the last 60s
per writer, summed across writers (`bench/scripts/extract_steady_throughput.py`).
**Failures:** 0 failed operations in every cell.

Artifact (chart): https://claude.ai/code/artifact/876ddd59-8466-47eb-be91-24d3f216e0a0

## Steady-state throughput (ops/sec)

| Workload | Writers | OzoneDB linearizable | Cassandra serial | Cassandra / OzoneDB |
|---|--:|--:|--:|--:|
| a (50r/50u) | 4 | 2,057 | 8,173 | 3.97 |
| a | 7 | 2,808 | 12,423 | 4.42 |
| a | 14 | 3,876 | 18,946 | 4.89 |
| b (95r/5u) | 4 | 4,091 | 9,403 | 2.30 |
| b | 7 | 5,838 | 14,732 | 2.52 |
| b | 14 | 9,002 | 23,218 | 2.58 |
| c (100r) | 4 | 6,361 | 9,502 | 1.49 |
| c | 7 | 10,638 | 15,144 | 1.42 |
| c | 14 | 20,338 | 23,939 | 1.18 |
| d (95r-latest/5ins) | 4 | 6,570 | 12,228 | 1.86 |
| d | 7 | 8,771 | 20,388 | 2.32 |
| d | 14 | 12,306 | 35,854 | 2.91 |
| f (read-modify-write) | 4 | 1,654 | 5,630 | 3.40 |
| f | 7 | 2,183 | 8,912 | 4.08 |
| f | 14 | 3,112 | 13,999 | 4.50 |

(OzoneDB also has a 2-writer point: a=1431, b=2450, c=3207, d=4432, f=1218. Cassandra
was not run at 2 writers.)

## Findings

1. **Cassandra serial beats OzoneDB linearizable on raw throughput at every point
   (4-14 writers), on every workload.** One 32-core server runs SERIAL reads and
   Paxos (LWT) writes for 14 concurrent clients without saturating.
2. **Read-only work (c) is the exception in trend.** The ratio falls 1.49 -> 1.18 as
   writers grow, because OzoneDB reads parallelize across the 7 client nodes
   (3.2x over 4->14 writers) while Cassandra scales 2.5x. The lines head toward a
   crossover past ~18 writers -- not reached in this run.
3. **Write-mixed work (a, d, f) diverges.** OzoneDB writes append to one shared log
   that a single Corfu sequencer orders globally, so write throughput is
   sequencer-bound and the gap widens with writers.
4. **The honest summary:** linearizable-over-shared-log is read-scalable but
   write-serialized. On a single beefy server, Cassandra's LWT path is faster in
   this writer range.

## Method notes and caveats

- **Tailer catch-up.** A fresh OzoneDB linearizable writer replays the whole log
  before its first fenced read: ~45s at 0 ops, then ~1000 ops/sec/writer. The
  reported number is the post-catch-up steady state, not YCSB's whole-run average.
- **Node amd207 excluded.** Its linearizable catch-up deadlocks (0 ops forever,
  single writer, survives a reboot). The sweep used the 7 good clients.
- **Points.** 4 / 7 / 14 writers (2 and 28 dropped for speed). The crossover and the
  point where Cassandra's single server saturates both need higher writer counts.
- **Not measured here** (from the earlier design discussion, where OzoneDB's case is
  stronger): per-core-normalized throughput, elasticity (writers join by opening the
  log), writer-failure behaviour, and measured staleness of the relaxed modes.

## Reproduce

```bash
# both datasets already loaded + snapshotted on amd217
bash bench/scripts/local/load_corfu_dataset.sh --writers 8          # OzoneDB dataset
bash bench/scripts/local/load_multinode_cassandra.sh --writers 8    # Cassandra dataset
# sweeps (7 good clients; see the tmp drivers or run per point):
bash bench/scripts/local/run_multinode_ycsb_with_corfu.sh --linearizable \
    --workloads "a b c d f" --writers-list 1 --client-hosts <first N> --duration 180 --run-tag TAG
bash bench/scripts/local/run_multinode_ycsb_with_cassandra.sh --consistency serial \
    --workloads "a b c d f" --writers-list 1 --client-hosts <first N> --duration 90 --run-tag TAG
python3 bench/scripts/extract_steady_throughput.py bench/results/local/TAG --window 60
```
