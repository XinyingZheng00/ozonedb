# Campaign strict100-20260830 (bench/PLAN-strict-100g.md)

One trial per invocation, three trials in all. Cassandra first, then OzoneDB,
because they share the server box. Edit OZONEDB_HOME at the top before a rerun.

Every cell is 300 s and starts both OzoneDB caches empty. The 16 GiB LRU
reaches about 3 % and the 50 GiB tier about 15 % by the end of a cell. That is
deliberate. Read the "Read this first" section of the plan before you read the
numbers.

The smoke cell of 2026-08-30 measured the tier fill directly: 2.44 GB in 120 s,
of which the first 38 s were tailer catch-up with no reads. That is 29.8 MB/s
of read time, which matches the 30 MB/s the plan predicted.

Launch detached from the laptop (macOS has no setsid):

    nohup bash bench/scripts/campaign-strict100/chain_trial.sh 1 \
      > bench/results/local/strict100-chains/trial1.log 2>&1 < /dev/null & disown

Watch it: tail -f bench/results/local/strict100-chains/trial1.log
Done when the log holds CHAIN-DONE. Failed when it holds CHAIN-FAILED.
