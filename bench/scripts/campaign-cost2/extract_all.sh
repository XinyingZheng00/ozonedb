#!/bin/bash
# PLAN-cost-2 Task 8: the three extractions, the two model runs, the overlay.
# Usage: extract_all.sh [10g]   -- "10g" runs the preliminary model on the 10 GB tags only.
set -euo pipefail
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
R=bench/results/local
EX="python3 bench/scripts/extract_cost_coefficients.py"
PY="uv run --quiet --with matplotlib python3"
PRICES=bench/scripts/plot/prices.json
SPACE=bench/scripts/plot/space.json
MODE=${1:-all}

if [[ "$MODE" == "10g" ]]; then
  $EX $R/$TAG-10g $R/$TAG-10g-tier $R/$TAG-cass-10g $R --window 60 --tsv bench/results-$TAG-10g-only.tsv
  $PY bench/scripts/plot/plot_cost_model.py bench/results-$TAG-10g-only.tsv $PRICES --space $SPACE \
    --tier-variant ch64k-adm --out-dir bench/scripts/plot/out-10g --table bench/results-$TAG-10g-only-projection.tsv
  exit 0
fi

# Only the tags that exist: the CloudLab lease ended 2026-08-29 04:17 before the
# 100 GB OzoneDB cells and the Cassandra zipf pair ran.
have() { for d in "$@"; do [[ -d "$d" ]] && printf '%s ' "$d"; done; }
$EX $(have $R/$TAG-10g $R/$TAG-10g-tier $R/$TAG-100g $R/$TAG-100g-tier $R/$TAG-cass-10g $R/$TAG-cass-100g) $R \
  --window 60 --tsv bench/results-$TAG.tsv
$EX $(have $R/$TAG-scale $R/$TAG-jni) --window 60 --tsv bench/results-$TAG-controls.tsv
# + the scrambled 10 GB tag: cpu_O comes from its workload-a cells (--cpu-workload a), the az cells were lost with the lease
$EX $(have $R/$TAG-10g-zipf $R/$TAG-100g-zipf $R/$TAG-cass-zipf $R/$TAG-cass-10g $R/$TAG-10g) $R --window 60 --tsv bench/results-$TAG-zipf.tsv

$PY bench/scripts/plot/plot_cost_model.py bench/results-$TAG.tsv $PRICES --space $SPACE \
  --tier-variant ch64k-adm --out-dir bench/scripts/plot/out --table bench/results-$TAG-projection.tsv
$PY bench/scripts/plot/plot_cost_model.py bench/results-$TAG-zipf.tsv $PRICES --space $SPACE \
  --tier-variant ch64k-adm --h-workload cz --cpu-workload a \
  --out-dir bench/scripts/plot/out-zipf --table bench/results-$TAG-zipf-projection.tsv
$PY bench/scripts/plot/overlay_projections.py bench/results-$TAG-projection.tsv \
  bench/results-$TAG-zipf-projection.tsv --out bench/results-$TAG.png --no-other-tier
echo "EXTRACT-DONE"
