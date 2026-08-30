#!/bin/bash
# PLAN-cost-2 Task 8: the extractions, the two model runs, the overlay.
#
# Corpus layout after the 2026-08-29 rerun (new cluster amd197 + amd189..amd200, the
# filter-copy fix 3b28a2cd on every OzoneDB cell):
#   results-$TAG.tsv          main corpus, one build: OzoneDB 10 GB (-10g-fix, -10g-fix-tier)
#                             and 100 GB (-100g, -100g-tier), Cassandra 10 GB and 100 GB.
#                             The load rows come from the results root (10 GB: amd127 load
#                             of 2026-08-28; 100 GB: amd197 load of 2026-08-29).
#   results-$TAG-controls.tsv scaling + JNI on the fixed build, Cassandra quorum a/c on the
#                             new box (-cass-10g-rerun).
#   results-$TAG-prefix.tsv   the 2026-08-28 amd127 cells on the build BEFORE the fix
#                             (-10g, -10g-tier, -scale, -jni): the "before" control.
#   results-$TAG-zipf.tsv     key-space zipfian: -10g-zipf (old build; h is unaffected),
#                             -100g-zipf, -cass-zipf, plus -cass-10g and -10g-fix so the
#                             model reads cpuC and cpuO (--cpu-workload a) from the fixed build.
# Usage: extract_all.sh
set -euo pipefail
export OZONEDB_HOME=/Users/oliver/Documents/UIUC/Research/ozone/ozonedb/.claude/worktrees/plan-cost-2
cd "$OZONEDB_HOME"
TAG=cost2-20260828
R=bench/results/local
EX="python3 bench/scripts/extract_cost_coefficients.py"
PY="uv run --quiet --with matplotlib python3"
PRICES=bench/scripts/plot/prices.json
SPACE=bench/scripts/plot/space.json

have() { for d in "$@"; do [[ -d "$d" ]] && printf '%s ' "$d"; done; }
$EX $(have $R/$TAG-10g-fix $R/$TAG-10g-fix-tier $R/$TAG-100g $R/$TAG-100g-tier $R/$TAG-cass-10g $R/$TAG-cass-100g) $R \
  --window 60 --tsv bench/results-$TAG.tsv
$EX $(have $R/$TAG-10g-fix-scale $R/$TAG-10g-fix-jni $R/$TAG-cass-10g-rerun) --window 60 --tsv bench/results-$TAG-controls.tsv
$EX $(have $R/$TAG-10g $R/$TAG-10g-tier $R/$TAG-scale $R/$TAG-jni) --window 60 --tsv bench/results-$TAG-prefix.tsv
$EX $(have $R/$TAG-10g-zipf $R/$TAG-100g-zipf $R/$TAG-cass-zipf $R/$TAG-cass-10g $R/$TAG-10g-fix) $R --window 60 --tsv bench/results-$TAG-zipf.tsv

$PY bench/scripts/plot/plot_cost_model.py bench/results-$TAG.tsv $PRICES --space $SPACE \
  --tier-variant ch64k-adm --out-dir bench/scripts/plot/out --table bench/results-$TAG-projection.tsv
$PY bench/scripts/plot/plot_cost_model.py bench/results-$TAG-zipf.tsv $PRICES --space $SPACE \
  --tier-variant ch64k-adm --h-workload cz --cpu-workload a \
  --out-dir bench/scripts/plot/out-zipf --table bench/results-$TAG-zipf-projection.tsv
$PY bench/scripts/plot/overlay_projections.py bench/results-$TAG-projection.tsv \
  bench/results-$TAG-zipf-projection.tsv --out bench/results-$TAG.png --no-other-tier
echo "EXTRACT-DONE"
