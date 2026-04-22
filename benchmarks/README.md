# Benchmarks

Four Go benchmark harnesses plus a Python plotter produce the data and charts for the project's performance story:

| Harness | What it measures | CSV |
|---|---|---|
| `kv_single_node` | Theseon vs Pebble on three YCSB-lite mixes | `out/kv_single_node.csv` |
| `kv_cluster` | 3-node cluster throughput + p99 under three quorum configs | `out/kv_cluster.csv` |
| `kv_chaos` | Throughput-over-time while node-2 is killed and restarted | `out/kv_chaos.csv` |
| `vector` | Recall@10 and latency sweep over `ef_search` on SIFT-1M | `out/vector.csv` |

All CSVs and chart PNGs are written under `benchmarks/out/`. The directory is gitignored.

---

## Step 0: Dataset — do this first

The vector benchmark needs the SIFT-1M dataset (~500 MB uncompressed). Start it **before** doing anything else so a slow mirror does not block you later.

```sh
mkdir -p benchmarks/data/sift
cd benchmarks/data/sift
curl -O 'ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz'
tar xzf sift.tar.gz
# expect: sift/sift_base.fvecs (~516 MB), sift_query.fvecs, sift_groundtruth.ivecs
```

If the mirror is unreachable, the harness accepts `--base`, `--query`, `--gt`, and `--max-base` flags so you can substitute another fvecs/ivecs dataset.

---

## Single-node KV (Theseon vs Pebble)

```sh
go run ./benchmarks/kv_single_node \
  --duration=60s \
  --keyspace-size=1000000 \
  --value-size=256 \
  --reps=3
```

Per `(engine, workload)`, the harness runs 3 reps with a fresh temp data directory, pre-fills the keyspace, then measures. Pre-fill does not count toward the benchmark numbers; the `AwaitReady` step between fill and measure waits for compactions to quiesce (Theseon) or explicitly flushes and compacts (Pebble) so both engines begin the measured window in the same LSM shape.

CSV columns: `engine,workload,rep,ops_per_sec,p50_ms,p95_ms,p99_ms,errors`.

---

## Cluster KV (3 nodes in-process)

```sh
go run ./benchmarks/kv_cluster \
  --duration=60s \
  --keyspace-size=100000 \
  --concurrency=8 \
  --reps=3
```

Three nodes live in the same process and communicate via real gRPC over TCP loopback. Three quorum configurations are swept: `(N=3, W=2, R=2)`, `(3,3,1)`, and `(3,1,3)`. The workload driver opens a gRPC client against node-1 (the coordinator) and drives `Put`/`Get`/`Delete` through the real replication path. Cluster pre-fill is slower than single-node pre-fill, so the default keyspace size is lower.

CSV columns: `N,W,R,workload,rep,ops_per_sec,p50_ms,p95_ms,p99_ms,errors`.

---

## Chaos

```sh
go run ./benchmarks/kv_chaos \
  --duration=120s \
  --kill-at=30s \
  --restart-at=60s \
  --concurrency=8
```

This runs steady YCSB-A load against a 3-node cluster. At `--kill-at`, node-2 is stopped. At `--restart-at`, a replacement is brought up using the same NodeID and the same data directory so it resumes from node-2’s prior local state, then rejoins via admin RPCs. Throughput is sampled every second along with any client-visible error rate and written to `out/kv_chaos.csv` with columns `t_seconds,ops_per_sec,error_rate,event`, where `event ∈ ("", "kill", "restart")`.

---

## Vector

```sh
go run ./benchmarks/vector                   # full SIFT-1M, default query count, ef sweep
go run ./benchmarks/vector --max-base=100000 # quick sanity run
```

This builds an HNSW index once (`M=16`, `EfConstruct=200` by default), then sweeps `ef_search ∈ {20, 50, 100, 200, 500, 1000}`. Recall@10 is computed against the provided ground-truth `sift_groundtruth.ivecs`.

CSV columns: `ef_search,recall_at_10,qps,p50_ms,p95_ms,p99_ms,num_queries`.

---

## Charts

`plot.py` can be run either with `uv` or with a standard Python virtual environment.

### Option A: uv (recommended)

`plot.py` includes inline dependency metadata, so `uv` can run it directly:

```sh
uv run benchmarks/plot.py
```

### Option B: standard Python / pip

```sh
python3 -m venv benchmarks/.venv
source benchmarks/.venv/bin/activate
pip install -r benchmarks/requirements.txt
python benchmarks/plot.py
```

It scans `benchmarks/out/` and writes one PNG per chart:

- `chart_kv_single_node.png` — throughput + p99, Theseon vs Pebble across 3 workloads.
- `chart_kv_cluster.png` — throughput + p99 across 3 quorum configs.
- `chart_kv_chaos.png` — throughput-over-time with the outage window highlighted and restart marked.
- `chart_vector_recall_qps.png` — recall@10 (y) vs QPS (x), curve over `ef_search`.
- `chart_vector_latency.png` — p95/p99 vs `ef_search` on a log axis.

If a CSV is missing, the corresponding chart is skipped.

---

## Full sweep

Use the convenience wrapper:

```sh
bash benchmarks/run-sweep.sh
bash benchmarks/run-sweep.sh --quick
```

The wrapper prefers `uv` for the plotting step and falls back to a standard Python virtual environment under `benchmarks/.venv` if `uv` is not installed.

If you want to run each step manually, use:

```sh
go run ./benchmarks/kv_single_node --duration=60s --keyspace-size=1000000 --reps=3
go run ./benchmarks/kv_cluster --duration=60s --keyspace-size=100000 --reps=3
go run ./benchmarks/kv_chaos --duration=120s --kill-at=30s --restart-at=60s
go run ./benchmarks/vector
uv run benchmarks/plot.py
```

If `uv` is not installed, render the charts with a standard Python environment instead:

```sh
python3 -m venv benchmarks/.venv
source benchmarks/.venv/bin/activate
pip install -r benchmarks/requirements.txt
python benchmarks/plot.py
```

Total runtime on an M1 Mac is roughly 90–120 minutes end-to-end depending on disk speed, with most of the time spent in single-node pre-fill and SIFT-1M HNSW indexing.
