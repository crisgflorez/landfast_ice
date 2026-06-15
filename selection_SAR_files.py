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
from pyproj import Transformer

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
#This means that we only consider files that have at least 30%  
# of the fast ice identified in the ice chart (nc file) in the day where we have ice chart and SAR file colocated
# in the previous or the following day. This is to ensure that we are only selecting files that have a good overlap with the originalice chart.

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
def get_key(base_file):
    base_file = base_file.replace(".SAFE.zip", "")
    parts = base_file.split("_")
    return "_".join(parts[:6])


nc_base_path = "/dmidata/projects/ai4arctic/asidv3/dataset_files"
nc_matches = []
for base_file in df_selected["base_file"]:
    key = get_key(base_file)
    pattern = os.path.join(nc_base_path, f"{key}_*.nc")
    matches = glob.glob(pattern)
    if len(matches) == 0:
        nc_matches.append(None)
    else:
        nc_matches.append(matches[0])
df_selected["nc_file_containing_ice_chart"] = nc_matches

df_selected.to_csv("/dmidata/users/cgf/files/selected_sar_files_v1.csv", index=False)



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

####Plot poster CPh
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

    # -------------------------
    # FORMATTING
    # -------------------------

    ax.axhline(0, color="black", linewidth=1)

    ax.set_title(f"{year}", fontsize=30)

    ax.set_ylabel("Δt (hours)", fontsize=30)

    # full year range
    ax.set_xlim(
        pd.Timestamp(f"{year}-01-01"),
        pd.Timestamp(f"{year}-12-31")
    )

    # optional fixed ylim
    ax.set_ylim(-24, 24)
    # Major ticks (with labels)
    ax.set_yticks([-24, 0, 24])

    # Minor ticks (without labels)
    ax.set_yticks([-12, -6, 6, 12], minor=True)

    # Tick sizes
    ax.tick_params(axis='y', which='major', labelsize=30, length=8)
    ax.tick_params(axis='y', which='minor', length=4, labelleft=False)

    # month labels
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))

    ax.tick_params(axis='x', labelsize=30)
    ax.tick_params(axis='y', labelsize=30)
    # Reference lines
    ax.axhline(0, color="black", linewidth=1)

    for y in [-12, 12]:
        ax.axhline(
            y,
            color="gray",
            linestyle="--",
            linewidth=1,
            alpha=0.7,
            zorder=0
        )

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
    bbox_to_anchor=(0.115, 0.975),
    fontsize=30
)

plt.tight_layout()
plt.show()


## Plot for the poster Cph
## Spatial distribution
df_new = df_selected.drop_duplicates(subset="folder", keep="first")
df_new["folder"] = df_new["folder"].dt.strftime("%Y%m%dT%H%M%S")

base_path_zarr = "/dmidata/projects/asip-cms/cgf/zarr_files2"
nc_base_path = "/dmidata/projects/asip-cms/cgf"

# Function to transform coordinates between different projections (using EPSG codes for the projections, e.g. 4326 for lat/lon, 3411 for North Polar Stereographic proj.)
def transform_points(x, y, fromEPSG, toEPSG):

    transformer = Transformer.from_crs(pyproj.CRS(f'EPSG:{fromEPSG}'), pyproj.CRS(f'EPSG:{toEPSG}'), always_xy=True)
    x, y = transformer.transform(x, y)

    return x, y

class NorthPolStere(cartopy.crs.Projection):
    def __init__(self):
        # see: http://www.spatialreference.org/ref/epsg/3413/
        proj4_params = {'proj': 'stere',
            'lat_0': 90.,
            'lon_0': -45,
            'lat_ts':70,
            'x_0': 0,
            'y_0': 0,
            'a':6378137,
            'b':298.257223563,
            'units': 'm',
            'datum':'WGS84',
            'no_defs': ''}

        super(NorthPolStere, self).__init__(proj4_params)

    @property
    def boundary(self):
        coords = ((self.x_limits[0], self.y_limits[0]),(self.x_limits[1], self.y_limits[0]),
                  (self.x_limits[1], self.y_limits[1]),(self.x_limits[0], self.y_limits[1]),
                  (self.x_limits[0], self.y_limits[0]))

        return cartopy.crs.sgeom.Polygon(coords).exterior

    @property
    def threshold(self):
        return 1e5

    @property
    def x_limits(self):
        return (-4000000,4000000)

    @property
    def y_limits(self):
        return (-5000000, 3000000)
    

fig, ax = plt.subplots(1, 1, figsize=(10, 10), subplot_kw={'projection': NorthPolStere()})
ax.set_facecolor('#6baed6')

world = gpd.read_file('/dmidata/projects/asip-cms/code/sentinel1_download/flood_data/ne_110m_admin_0_countries.shp')
world = world[world['SOVEREIGNT'] != 'Antarctica']
world.to_crs(epsg=3411).plot(ax=ax, color='#6B4F3A', alpha=1)
world.boundary.to_crs(epsg=3411).plot(ax=ax, color='white', linewidth=0.5, zorder=3)

for i, (_, row) in enumerate(df_new.iterrows()):
    folder = row["folder"]      # por ejemplo: 20180627T120051
    nc = row["nc_file_containing_ice_chart"]

    # Carpeta que quieres comprobar
    folder_path = os.path.join(base_path_zarr, folder)

    if os.path.isdir(folder_path):

        print(f"Opening: {nc}")

        ds = xr.open_dataset(nc)
        #In the netcdf file the coordinates of the CGPs are saved in 
        #ds.sar_grid_latitude and ds.sar_grid_longitude
        #These values have been already corrected to be at sea level and are in EPSG:4326 (lat/lon), so we need to transform them to 3411 and upsample the GCP grid to have a coordinate for each pixel in the SAR image, which is needed for the resampling and for plotting later on.
        # We transform to 3411 because we need a planar projection to set the GCPs to sea level and to upsample the GCP grid accurately (too much distortion in lat/lon). 
        x, y = transform_points(ds.sar_grid_longitude.values, ds.sar_grid_latitude.values, fromEPSG=4326, toEPSG=3411)
        # We create a convex hull around the GCPs to visualize the area covered by the SAR image in the plot later on.
        points = np.column_stack([x.flatten(), y.flatten()])
        footprint = MultiPoint(points).convex_hull
        ax.add_geometries(
            [footprint],
            crs=NorthPolStere(),
            facecolor='none',
            edgecolor='red',
            linewidth=1.2,
            zorder=10
        )        

        # Aquí haces lo que necesites con ds
        ds.close()

    else:
        print(f"Folder does not exist: {folder_path}")