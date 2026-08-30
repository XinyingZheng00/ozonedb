# Campaign cost2-20260828 drivers (bench/PLAN-cost-2.md)

The chains that ran the campaign, as run. Each sets OZONEDB_HOME to the worktree it ran
from and logs to bench/results/local/cost2-chains/; edit those two paths before a rerun.
Launch from the laptop with `nohup bash chain_X.sh > log 2>&1 < /dev/null & disown`
(no setsid on macOS). chain_oz100.sh takes the load writer count (16) and is the one
to resume from its RAM-cells step on a new experiment. summarize.py prints one line
per cell from the extractor; extract_all.sh is Task 8.
