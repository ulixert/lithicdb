#!/usr/bin/env bash
# Full benchmark sweep. Runs the four harnesses in sequence, writes CSVs
# to benchmarks/out/, then re-renders the charts. Designed to survive
# one-off failures in a single harness — the others still run.
#
# Usage: bash benchmarks/run-sweep.sh [--quick]
#   --quick  use reduced keyspace/duration for a ~10-min sanity sweep

set -u  # unset-variable is an error, but don't exit on command failure

QUICK=0
if [[ "${1:-}" == "--quick" ]]; then
  QUICK=1
fi

if [[ $QUICK -eq 1 ]]; then
  SN_DURATION=20s; SN_KEYSPACE=50000;   SN_REPS=2
  CL_DURATION=20s; CL_KEYSPACE=10000;   CL_REPS=2
  CH_DURATION=60s; CH_KILL=20s;         CH_RESTART=40s
  VEC_NUMQ=200;    VEC_MAXBASE=100000
else
  SN_DURATION=60s;  SN_KEYSPACE=2000000; SN_REPS=3
  CL_DURATION=60s;  CL_KEYSPACE=100000;  CL_REPS=3
  CH_DURATION=180s; CH_KILL=60s;         CH_RESTART=120s
  VEC_NUMQ=5000;    VEC_MAXBASE=0
fi

mkdir -p benchmarks/out
rm -f benchmarks/out/*.csv benchmarks/out/*.png
LOG=benchmarks/out/sweep.log
: > "$LOG"

ts() { date "+%H:%M:%S"; }
say() { echo "[$(ts)] $*" | tee -a "$LOG"; }

say "=== starting full sweep ==="
say "single-node: duration=$SN_DURATION keyspace=$SN_KEYSPACE reps=$SN_REPS"
say "cluster:     duration=$CL_DURATION keyspace=$CL_KEYSPACE reps=$CL_REPS"
say "chaos:       duration=$CH_DURATION kill=$CH_KILL restart=$CH_RESTART"
say "vector:      num-queries=$VEC_NUMQ max-base=$VEC_MAXBASE"

say ">>> kv_single_node"
go run ./benchmarks/kv_single_node \
  --duration="$SN_DURATION" --keyspace-size="$SN_KEYSPACE" \
  --value-size=256 --reps="$SN_REPS" \
  --out=benchmarks/out/kv_single_node.csv 2>&1 | tee -a "$LOG"
say "<<< kv_single_node done"

say ">>> kv_cluster"
go run ./benchmarks/kv_cluster \
  --duration="$CL_DURATION" --keyspace-size="$CL_KEYSPACE" \
  --value-size=256 --reps="$CL_REPS" --concurrency=8 \
  --out=benchmarks/out/kv_cluster.csv 2>&1 | tee -a "$LOG"
say "<<< kv_cluster done"

say ">>> kv_chaos"
go run ./benchmarks/kv_chaos \
  --duration="$CH_DURATION" --kill-at="$CH_KILL" --restart-at="$CH_RESTART" \
  --keyspace-size=50000 --value-size=256 --concurrency=8 \
  --out=benchmarks/out/kv_chaos.csv 2>&1 | tee -a "$LOG"
say "<<< kv_chaos done"

say ">>> vector"
VEC_ARGS=(--num-queries="$VEC_NUMQ" --out=benchmarks/out/vector.csv)
if [[ $VEC_MAXBASE -gt 0 ]]; then
  VEC_ARGS+=(--max-base="$VEC_MAXBASE")
fi
go run ./benchmarks/vector "${VEC_ARGS[@]}" 2>&1 | tee -a "$LOG"
say "<<< vector done"

say ">>> plot.py"
if command -v uv >/dev/null 2>&1; then
  uv run benchmarks/plot.py 2>&1 | tee -a "$LOG"
elif command -v python3 >/dev/null 2>&1; then
  if [[ ! -x benchmarks/.venv/bin/python ]]; then
    python3 -m venv benchmarks/.venv 2>&1 | tee -a "$LOG"
  fi
  benchmarks/.venv/bin/python -m pip install -r benchmarks/requirements.txt 2>&1 | tee -a "$LOG"
  benchmarks/.venv/bin/python benchmarks/plot.py 2>&1 | tee -a "$LOG"
else
  say "plot skipped: need either uv or python3"
fi
say "<<< plot done"

say "=== sweep complete; CSVs + PNGs in benchmarks/out/ ==="
ls -la benchmarks/out/ | tee -a "$LOG"
