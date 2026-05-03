### Workload A
_Workload A (50% read, 50% update)_  
Throughput (ops/sec; mean ± stddev across repeats); — = missing.

| System | T=1 | T=2 | T=4 | T=8 | T=16 |
|---|---|---|---|---|---|
| OzoneKV | 7,152 | 12,948 ± 56 | 23,538 ± 121 | 38,395 ± 497 | 49,149 ± 723 |
| RocksDB | 8,722 | 66 ± 0 | 64 ± 0 | 62 ± 0 | 58 ± 0 |
| SQLite (trunk, plain WAL) | 7,450 | 0 ± 0 | — | — | — |
| SQLite (BEGIN CONCURRENT + WAL2) | 7,379 | 0 ± 0 | — | 1 ± 0 | — |
| SQLite (HCTree) | 30,565 | 35,391 ± 1,336 | 33,485 ± 857 | 32,119 ± 128 | 31,411 ± 293 |
