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

import matplotlib.pyplot as plt
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


