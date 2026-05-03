### Workload F
_Workload F (50% read, 50% read-modify-write)_  
Throughput (ops/sec; mean ± stddev across repeats); — = missing.

| System | T=1 | T=2 | T=4 | T=8 | T=16 |
|---|---|---|---|---|---|
| OzoneKV | 6,597 | 11,825 ± 30 | 21,900 ± 53 | 36,041 ± 321 | 46,327 ± 1,302 |
| RocksDB | 8,488 | 44 ± 0 | 43 ± 0 | 41 ± 0 | 39 ± 0 |
| SQLite (trunk, plain WAL) | 7,380 | 0 ± 0 | — | 1 | — |
| SQLite (BEGIN CONCURRENT + WAL2) | 7,146 | 0 | 0 ± 0 | — | 1 |
| SQLite (HCTree) | 27,761 | 36,311 ± 422 | 34,041 ± 221 | 31,700 ± 112 | 30,020 ± 936 |
