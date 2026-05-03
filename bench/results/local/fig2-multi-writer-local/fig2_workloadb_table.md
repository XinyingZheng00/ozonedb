### Workload B
_Workload B (95% read, 5% update)_  
Throughput (ops/sec; mean ± stddev across repeats); — = missing.

| System | T=1 | T=2 | T=4 | T=8 | T=16 |
|---|---|---|---|---|---|
| OzoneKV | 15,803 | 29,676 ± 215 | 57,289 ± 179 | 105,655 ± 445 | 160,980 ± 257 |
| RocksDB | 26,659 | 70 ± 0 | 69 ± 0 | 66 ± 0 | 61 ± 0 |
| SQLite (trunk, plain WAL) | 25,178 | 1 ± 0 | 2 ± 1 | 5 ± 0 | 10 ± 0 |
| SQLite (BEGIN CONCURRENT + WAL2) | 25,043 | 1 ± 1 | 3 ± 1 | 5 ± 0 | 10 ± 1 |
| SQLite (HCTree) | 102,953 | 197,129 ± 1,831 | 320,471 ± 1,435 | 344,395 ± 1,400 | 322,506 ± 1,899 |
