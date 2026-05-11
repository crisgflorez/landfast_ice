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


# datetime conversion
df_all["folder"] = pd.to_datetime(df_all["folder"], format="%Y%m%dT%H%M%S")

df_all["year"] = df_all["folder"].dt.year
df_all["date"] = df_all["folder"].dt.date
df_all["date"] = pd.to_datetime(df_all["date"])
df_all["direction"] = np.where(df_all["time_diff_hours"] > 0, "next", "prev")
df_all["abs_time"] = df_all["time_diff_hours"].abs()

# before/after sign
df_all["sign"] = np.where(df_all["time_diff_hours"] > 0, "pos", "neg")

# =========================
# SETTINGS
# =========================

colors = {
    "RCM": "orange",
    "S1": "blue"
}

years = sorted(df_all["year"].unique())

fig, axes = plt.subplots(len(years), 1, figsize=(14, 4 * len(years)))

if len(years) == 1:
    axes = [axes]

# =========================
# PLOT PER YEAR
# =========================

for ax, year in zip(axes, years):

    df_y = df_all[df_all["year"] == year]

    grouped = df_y.groupby(["date", "sensor", "sign"]).size().reset_index(name="count")

    # =========================
    # POSITIVE
    # =========================
    pos = grouped[grouped["sign"] == "pos"].pivot(
        index="date", columns="sensor", values="count"
    ).fillna(0)

    bottom = np.zeros(len(pos))

    for sensor in ["RCM", "S1"]:
        if sensor in pos.columns:
            ax.bar(
                pos.index,
                pos[sensor],
                bottom=bottom,
                color=colors[sensor]
            )
            bottom += pos[sensor].values

    # =========================
    # NEGATIVE
    # =========================
    neg = grouped[grouped["sign"] == "neg"].pivot(
        index="date", columns="sensor", values="count"
    ).fillna(0)

    bottom = np.zeros(len(neg))

    for sensor in ["RCM", "S1"]:
        if sensor in neg.columns:
            ax.bar(
                neg.index,
                -neg[sensor],
                bottom=-bottom,
                color=colors[sensor]
            )
            bottom += neg[sensor].values

    # =========================
    # FORMATTING
    # =========================

    ax.axhline(0, color="black", linewidth=1)

    ax.set_title(f"{year}", fontsize=22)

    ax.set_ylabel("Nb SAR files\n before and after", fontsize=22)

    # -------------------------
    # FORCE FULL YEAR RANGE
    # -------------------------
    ax.set_xlim(
        pd.Timestamp(f"{year}-01-01"),
        pd.Timestamp(f"{year}-12-31")
    )

    # -------------------------
    # MONTH LABELS (Jan, Feb...)
    # -------------------------
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))

    # tick styling
    ax.tick_params(axis='x', labelsize=22)
    ax.tick_params(axis='y', labelsize=22)

    plt.setp(ax.get_xticklabels(), rotation=0, ha="center")

# =========================
# LEGEND (GLOBAL)
# =========================

handles = [
    plt.Rectangle((0, 0), 1, 1, color="orange"),
    plt.Rectangle((0, 0), 1, 1, color="blue")
]

fig.legend(
    handles,
    ["RCM", "S1"],
    loc="upper right",
    bbox_to_anchor=(0.3, 0.95),
    fontsize=22
)
plt.tight_layout()
plt.show()



# function to compute stats per group
def summarize(group):
    return pd.Series({
        # NC overlap stats
        "mean_overlap_nc": group["overlap_nc_pct"].mean(),
        "median_overlap_nc": group["overlap_nc_pct"].median(),
        "std_overlap_nc": group["overlap_nc_pct"].std(),
        "max_overlap_nc": group["overlap_nc_pct"].max(),
        "min_overlap_nc": group["overlap_nc_pct"].min(),

        # SAR overlap stats
        "mean_overlap_sar": group["overlap_sar_pct"].mean(),
        "median_overlap_sar": group["overlap_sar_pct"].median(),
        "std_overlap_sar": group["overlap_sar_pct"].std(),
        "max_overlap_sar": group["overlap_sar_pct"].max(),
        "min_overlap_sar": group["overlap_sar_pct"].min(),

        # TIME stats (NEW)
        "mean_time": group["abs_time"].mean(),
        "min_time": group["abs_time"].min(),
        "max_time": group["abs_time"].max(),
        "std_time": group["abs_time"].std(),

        # auxiliary info
        "n_files": len(group)
    })

stats = df_all.groupby(["folder", "direction"]).apply(summarize).reset_index()

stats_next = stats[stats["direction"] == "next"].sort_values("folder")
stats_prev = stats[stats["direction"] == "prev"].sort_values("folder")


fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

# =========================
# NEXT
# =========================
axes[0].plot(stats_next["folder"], stats_next["mean_overlap_nc"], label="Mean percent. fast ice identified in the original nc file is covered in these new files", linewidth=2)
axes[0].plot(stats_next["folder"], stats_next["max_overlap_nc"], label="Max percent. fast ice identified in the original nc file is covered in these new files", linestyle="--")

axes[0].set_title("NEXT (after base SAR)")
axes[0].set_ylabel("Overlap (%)")
axes[0].legend()
axes[0].grid()

# =========================
# PREV
# =========================
axes[1].plot(stats_prev["folder"], stats_prev["mean_overlap_nc"], label="Mean percent. fast ice identified in the original nc file is covered in these new files", linewidth=2)
axes[1].plot(stats_prev["folder"], stats_prev["max_overlap_nc"], label="Max percent. fast ice identified in the original nc file is covered in these new files", linestyle="--")

axes[1].set_title("PREV (before base SAR)")
axes[1].set_ylabel("Overlap (%)")
axes[1].set_xlabel("Date")
axes[1].legend()
axes[1].grid()

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

# =========================
# NEXT
# =========================
axes[0].plot(stats_next["folder"], stats_next["mean_overlap_sar"], label="Mean percent. fast ice in these new files", linewidth=2)
axes[0].plot(stats_next["folder"], stats_next["max_overlap_sar"], label="Max percent. fast ice in these new files", linestyle="--")

axes[0].set_title("NEXT (after base SAR)")
axes[0].set_ylabel("Overlap (%)")
axes[0].legend()
axes[0].grid()

# =========================
# PREV
# =========================
axes[1].plot(stats_prev["folder"], stats_prev["mean_overlap_sar"], label="Mean percent. fast ice in these new files", linewidth=2)
axes[1].plot(stats_prev["folder"], stats_prev["max_overlap_sar"], label="Max percent. fast ice in these new files", linestyle="--")

axes[1].set_title("PREV (before base SAR)")
axes[1].set_ylabel("Overlap (%)")
axes[1].set_xlabel("Date")
axes[1].legend()
axes[1].grid()

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()





fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

# =========================
# NEXT
# =========================
axes[0].plot(stats_next["folder"], stats_next["mean_time"], label="Mean time between new SAR images and nc file", linewidth=2)
axes[0].plot(stats_next["folder"], stats_next["max_time"], label="Max time between new SAR images and nc file", linestyle="--")

axes[0].set_title("NEXT (after base SAR)")
axes[0].set_ylabel("Time (hours)")
axes[0].legend()
axes[0].grid()

# =========================
# PREV
# =========================
axes[1].plot(stats_prev["folder"], stats_prev["mean_time"], label="Mean time between new SAR images and nc file", linewidth=2)
axes[1].plot(stats_prev["folder"], stats_prev["max_time"], label="Max time between new SAR images and nc file", linestyle="--")

axes[1].set_title("PREV (before base SAR)")
axes[1].set_ylabel("Time (hours)")
axes[1].set_xlabel("Date")
axes[1].legend()
axes[1].grid()

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()



