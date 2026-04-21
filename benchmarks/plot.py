# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "matplotlib>=3.7",
#   "numpy>=1.24",
#   "pandas>=2.0",
# ]
# ///

"""Render benchmark charts from CSV output.

Usage:
    uv run benchmarks/plot.py

Reads CSV files from benchmarks/out/ and writes PNG files next to them.
"""

from __future__ import annotations

import sys
from pathlib import Path
from collections.abc import Callable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

OUT_DIR = Path("benchmarks/out")
WORKLOADS = ["YCSB-A", "YCSB-B", "YCSB-C"]
ENGINE_ORDER = ["theseon", "pebble"]
ENGINE_COLORS = {
    "theseon": "#2E86AB",
    "pebble": "#A23B72",
}


def load_csv(name: str) -> pd.DataFrame | None:
    """Load a CSV from the benchmark output directory."""
    path = OUT_DIR / name
    if not path.exists():
        print(f"[skip] missing {path}", file=sys.stderr)
        return None
    return pd.read_csv(path)

def save_figure(fig: plt.Figure, name: str) -> None:
    """Save and close a figure."""
    out_path = OUT_DIR / name
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"[ok] wrote {out_path}")
    plt.close(fig)


def require_columns(df: pd.DataFrame, required: list[str], chart_name: str) -> None:
    """Raise a clear error if required columns are missing."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{chart_name}: missing columns: {', '.join(missing)}")


def style_axes(ax: plt.Axes, *, ylabel: str, xlabel: str | None = None) -> None:
    """Apply consistent axis styling."""
    if xlabel is not None:
        ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)


def chart_kv_single_node(df: pd.DataFrame) -> None:
    """Render throughput and p99 latency for Theseon vs Pebble."""
    require_columns(
        df,
        ["engine", "workload", "ops_per_sec", "p99_ms"],
        "chart_kv_single_node",
    )

    agg = (
        df.groupby(["engine", "workload"], as_index=False)
        .agg(
            ops_per_sec=("ops_per_sec", "median"),
            p99_ms=("p99_ms", "median"),
        )
    )

    fig, (ax_ops, ax_p99) = plt.subplots(1, 2, figsize=(12, 4.5))
    x = np.arange(len(WORKLOADS))
    width = 0.36

    for i, engine in enumerate(ENGINE_ORDER):
        subset = agg[agg["engine"] == engine].set_index("workload").reindex(WORKLOADS)
        offset = (i - 0.5) * width
        color = ENGINE_COLORS[engine]

        ax_ops.bar(
            x + offset,
            subset["ops_per_sec"],
            width,
            label=engine,
            color=color,
        )
        ax_p99.bar(
            x + offset,
            subset["p99_ms"],
            width,
            label=engine,
            color=color,
        )

    for ax, ylabel in (
        (ax_ops, "throughput (ops/sec)"),
        (ax_p99, "p99 latency (ms)"),
    ):
        ax.set_xticks(x)
        ax.set_xticklabels(WORKLOADS)
        style_axes(ax, ylabel=ylabel)
        ax.legend()

    ax_ops.set_title("Single-node throughput: Theseon vs Pebble")
    ax_p99.set_title("Single-node p99 latency: Theseon vs Pebble")

    fig.tight_layout()
    save_figure(fig, "chart_kv_single_node.png")


def chart_kv_cluster(df: pd.DataFrame) -> None:
    """Render cluster throughput and p99 latency by quorum configuration."""
    require_columns(
        df,
        ["N", "W", "R", "workload", "ops_per_sec", "p99_ms"],
        "chart_kv_cluster",
    )

    working = df.copy()
    working["quorum"] = (
        "N=" + working["N"].astype(str)
        + ",W=" + working["W"].astype(str)
        + ",R=" + working["R"].astype(str)
    )

    agg = (
        working.groupby(["quorum", "workload"], as_index=False)
        .agg(
            ops_per_sec=("ops_per_sec", "median"),
            p99_ms=("p99_ms", "median"),
        )
    )

    quorums = working["quorum"].drop_duplicates().tolist()
    if not quorums:
        raise ValueError("chart_kv_cluster: no quorum configurations found")

    fig, (ax_ops, ax_p99) = plt.subplots(1, 2, figsize=(12, 4.5))
    x = np.arange(len(WORKLOADS))
    width = 0.8 / len(quorums)
    palette = plt.cm.viridis(np.linspace(0.2, 0.8, len(quorums)))

    for i, quorum in enumerate(quorums):
        subset = agg[agg["quorum"] == quorum].set_index("workload").reindex(WORKLOADS)
        offset = (i - (len(quorums) - 1) / 2) * width

        ax_ops.bar(
            x + offset,
            subset["ops_per_sec"],
            width,
            label=quorum,
            color=palette[i],
        )
        ax_p99.bar(
            x + offset,
            subset["p99_ms"],
            width,
            label=quorum,
            color=palette[i],
        )

    for ax, ylabel in (
        (ax_ops, "throughput (ops/sec)"),
        (ax_p99, "p99 latency (ms)"),
    ):
        ax.set_xticks(x)
        ax.set_xticklabels(WORKLOADS)
        style_axes(ax, ylabel=ylabel)
        ax.legend(title="quorum")

    ax_ops.set_title("Cluster throughput by quorum configuration")
    ax_p99.set_title("Cluster p99 latency by quorum configuration")

    fig.tight_layout()
    save_figure(fig, "chart_kv_cluster.png")


def chart_kv_chaos(df: pd.DataFrame) -> None:
    """Render throughput over time with outage window and optional error rate."""
    require_columns(
        df,
        ["t_seconds", "ops_per_sec"],
        "chart_kv_chaos",
    )

    fig, ax_left = plt.subplots(figsize=(12, 4.8))
    ops_color = ENGINE_COLORS["theseon"]

    ax_left.plot(
        df["t_seconds"],
        df["ops_per_sec"],
        color=ops_color,
        lw=1,
        alpha=0.35,
        label="ops/sec (1s samples)",
    )

    rolling = df["ops_per_sec"].rolling(window=5, center=True, min_periods=1).mean()
    ax_left.plot(
        df["t_seconds"],
        rolling,
        color=ops_color,
        lw=2.4,
        label="ops/sec (5s rolling mean)",
    )

    ax_left.set_xlabel("time (seconds)")
    ax_left.set_ylabel("throughput (ops/sec)", color=ops_color)
    ax_left.tick_params(axis="y", labelcolor=ops_color)
    ax_left.grid(alpha=0.3)
    ax_left.set_axisbelow(True)

    ax_right: plt.Axes | None = None
    if "error_rate" in df.columns and (df["error_rate"] > 0).any():
        ax_right = ax_left.twinx()
        ax_right.plot(
            df["t_seconds"],
            df["error_rate"] * 100,
            color="#D7263D",
            lw=1.4,
            linestyle="--",
            label="error rate (%)",
        )
        ax_right.set_ylabel("error rate (%)", color="#D7263D")
        ax_right.tick_params(axis="y", labelcolor="#D7263D")

    events = (
        df["event"].fillna("").astype(str)
        if "event" in df.columns
        else pd.Series("", index=df.index)
    )

    kill_rows = df.loc[events == "kill", "t_seconds"]
    restart_rows = df.loc[events == "restart", "t_seconds"]

    if not kill_rows.empty and not restart_rows.empty:
        kill_t = float(kill_rows.iloc[0])
        restart_t = float(restart_rows.iloc[0])

        ax_left.axvspan(kill_t, restart_t, alpha=0.12, color="#D7263D", zorder=0)
        ax_left.axvline(kill_t, color="#8B0000", linestyle=":", alpha=0.7)
        ax_left.axvline(restart_t, color="#2E7D32", linestyle=":", alpha=0.7)

        y_top = ax_left.get_ylim()[1]
        ax_left.text(
            (kill_t + restart_t) / 2,
            y_top * 0.96,
            "node-2 unavailable",
            ha="center",
            va="top",
            fontsize=10,
            color="#8B0000",
            style="italic",
        )
        ax_left.text(
            kill_t,
            y_top * 0.88,
            f"kill (t={kill_t:.0f}s)",
            ha="left",
            va="top",
            fontsize=9,
            color="#8B0000",
        )
        ax_left.text(
            restart_t,
            y_top * 0.88,
            f"restart/drain (t={restart_t:.0f}s)",
            ha="left",
            va="top",
            fontsize=9,
            color="#2E7D32",
        )

    lines, labels = ax_left.get_legend_handles_labels()
    if ax_right is not None:
        right_lines, right_labels = ax_right.get_legend_handles_labels()
        lines.extend(right_lines)
        labels.extend(right_labels)
    ax_left.legend(lines, labels, loc="best")

    ax_left.set_title("Chaos run: node-2 killed and restarted mid-load")

    fig.tight_layout()
    save_figure(fig, "chart_kv_chaos.png")
