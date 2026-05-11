import os
from CDS import CDS
from datetime import datetime, timedelta
from shapely.geometry import Polygon
from dotenv import load_dotenv
from eoutils import S1Processor, RCMProcessor
from EODMS import EODMS
import json
import pandas as pd
from shapely.geometry import shape, Point, MultiPoint, MultiPolygon
from shapely.ops import transform, unary_union
import shapely
from pyproj import Transformer
import xarray as xr
from tqdm import tqdm
import re
import dateutil
import numpy as np
from tqdm import tqdm
import cartopy
import matplotlib.pyplot as plt
import geopandas as gpd
import glob
import matplotlib.dates as mdates

# =========================
# LOAD DATA
# =========================

df = pd.read_csv("/dmidata/users/cgf/files/overlap_results.csv")
df["sensor"] = "S1"

df_rcm = pd.read_csv("/dmidata/users/cgf/files/overlap_results_rcm.csv")
df_rcm["sensor"] = "RCM"

df_all = pd.concat([df, df_rcm], ignore_index=True)
df_all = df_all.sort_values("folder")
df_all = df_all.reset_index(drop=True)

df_all["direction"] = np.where(df_all["time_diff_hours"] > 0, "next", "prev")

# datetime conversion
df_all["folder"] = pd.to_datetime(df_all["folder"], format="%Y%m%dT%H%M%S")

df_all["year"] = df_all["folder"].dt.year
df_all["date"] = df_all["folder"].dt.date


# =========================
# SELECT BEST FILE PER FOLDER
# =========================

threshold = 30  # minimum overlap_nc_pct

selected_rows = []

for folder, group in df_all.groupby("folder"):

    # -------------------------
    # NEXT files
    # -------------------------
    next_group = group[
        (group["direction"] == "next") &
        (group["overlap_nc_pct"] > threshold)
    ]

    if len(next_group) > 0:

        # choose smallest absolute time difference
        best_next = next_group.loc[
            next_group["time_diff_hours"].abs().idxmin()
        ]

        selected_rows.append(best_next)

    # -------------------------
    # PREV files
    # -------------------------
    prev_group = group[
        (group["direction"] == "prev") &
        (group["overlap_nc_pct"] > threshold)
    ]

    if len(prev_group) > 0:

        best_prev = prev_group.loc[
            prev_group["time_diff_hours"].abs().idxmin()
        ]

        selected_rows.append(best_prev)

# =========================
# FINAL DATAFRAME
# =========================

df_selected = pd.DataFrame(selected_rows)

print(df_selected.head())


years = sorted(df_selected["year"].unique())

fig, axes = plt.subplots(len(years), 1, figsize=(14, 4 * len(years)))

if len(years) == 1:
    axes = [axes]

# =========================
# PLOT PER YEAR
# =========================

for ax, year in zip(axes, years):

    df_y = df_selected[df_selected["year"] == year]

    # -------------------------
    # PLOT VERTICAL LINES
    # -------------------------
    for sensor in ["RCM", "S1"]:

        df_s = df_y[df_y["sensor"] == sensor]

        # =====================
        # VERTICAL LINES
        # =====================

        if sensor == "RCM":

            ax.vlines(
                x=df_s["folder"],
                ymin=0,
                ymax=df_s["time_diff_hours"],
                color="orange",
                linewidth=4,
                linestyle="--",
                alpha=0.9,
                label="RCM" if year == years[0] else ""
            )

        elif sensor == "S1":

            ax.vlines(
                x=df_s["folder"],
                ymin=0,
                ymax=df_s["time_diff_hours"],
                color="blue",
                linewidth=1.2,
                linestyle="-",
                alpha=0.8,
                label="S1" if year == years[0] else ""
            )

        # =====================
        # FIXED MARKERS
        # =====================

        # NEXT
        df_next = df_s[df_s["time_diff_hours"] > 0]

        # PREV
        df_prev = df_s[df_s["time_diff_hours"] < 0]

        # marker style
        marker_size = 2

        # NEXT markers at y = +26
        ax.scatter(
            df_next["folder"],
            np.full(len(df_next), 26),
            color="orange" if sensor == "RCM" else "blue",
            s=marker_size,
            zorder=5
        )

        # PREV markers at y = -26
        ax.scatter(
            df_prev["folder"],
            np.full(len(df_prev), -26),
            color="orange" if sensor == "RCM" else "blue",
            s=marker_size,
            zorder=5
        )

    # -------------------------
    # FORMATTING
    # -------------------------

    ax.axhline(0, color="black", linewidth=1)

    ax.set_title(f"{year}", fontsize=18)

    ax.set_ylabel("Time difference (hours)", fontsize=20)

    # full year range
    ax.set_xlim(
        pd.Timestamp(f"{year}-01-01"),
        pd.Timestamp(f"{year}-12-31")
    )

    # optional fixed ylim
    ax.set_ylim(-30, 30)

    # month labels
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))

    ax.tick_params(axis='x', labelsize=20)
    ax.tick_params(axis='y', labelsize=20)

    plt.setp(ax.get_xticklabels(), rotation=0, ha="center")

# =========================
# GLOBAL LEGEND
# =========================

handles = [
    plt.Line2D([0], [0], color="orange", lw=3, linestyle="--"),
    plt.Line2D([0], [0], color="blue", lw=1.5, linestyle="-")
]

fig.legend(
    handles,
    ["RCM", "S1"],
    loc="upper left",
    bbox_to_anchor=(0.1, 0.95),
    fontsize=20
)

plt.tight_layout()
plt.show()
