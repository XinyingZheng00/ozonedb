### Workload D
_Workload D (95% read, 5% insert, latest dist.)_  
Throughput (ops/sec; mean ± stddev across repeats); — = missing.

| System | T=1 | T=2 | T=4 | T=8 | T=16 |
|---|---|---|---|---|---|
| OzoneKV | 25,211 | 44,192 ± 177 | 79,276 ± 319 | 137,414 ± 304 | 200,950 ± 687 |
| RocksDB | 46,850 | 71 ± 0 | 69 ± 1 | 67 ± 0 | 61 ± 1 |
| SQLite (trunk, plain WAL) | 30,176 | 1 ± 0 | 3 ± 0 | 5 ± 1 | 10 ± 1 |
| SQLite (BEGIN CONCURRENT + WAL2) | 29,313 | 1 ± 0 | 2 ± 1 | 5 ± 1 | 12 ± 2 |
| SQLite (HCTree) | 102,984 | 197,445 ± 557 | 320,273 ± 464 | 350,215 ± 1,755 | 331,890 ± 1,137 |
