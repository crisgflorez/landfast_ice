import os
import glob
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from eoutils import S1Processor
import numpy as np
from matplotlib.colors import BoundaryNorm

# Open the text file
with open("/dmidata/users/cgf/landfast_ice/matching_files.txt", "r") as f:
    lines = f.readlines()

# Filter lines containing both "SouthWest" and "202102"
filtered = [line.strip() for line in lines if "CentralEast" in line and "202102" in line]

# Show results
for line in filtered:
    print(line)

####
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082107_20210208T082207_036496_044911_CA5C_icechart_dmi_202102080820_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183610_20210210T183710_025548_030B57_8C33_icechart_dmi_202102101835_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210220T082107_20210220T082207_036671_044F2C_7946_icechart_dmi_202102200820_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210222T080444_20210222T080544_036700_04501C_41D8_icechart_dmi_202102220805_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210201T082819_20210201T082919_036394_044588_A575_icechart_dmi_202102010830_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210220T082007_20210220T082107_036671_044F2C_752F_icechart_dmi_202102200820_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210225T082918_20210225T083018_036744_0451AF_77C4_icechart_dmi_202102250830_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082007_20210208T082107_036496_044911_8C01_icechart_dmi_202102080820_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210222T080544_20210222T080644_036700_04501C_5854_icechart_dmi_202102220805_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210201T082919_20210201T083019_036394_044588_5DEC_icechart_dmi_202102010830_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183506_20210210T183610_025548_030B57_6234_icechart_dmi_202102101835_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210225T082818_20210225T082918_036744_0451AF_3E3F_icechart_dmi_202102250830_CentralEast_RIC.nc
#/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210214T185202_20210214T185306_036590_044C66_CBC6_icechart_dmi_202102141850_CentralEast_RIC.nc
####

files = [
    # 08/02/2021 _1
    '/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082107_20210208T082207_036496_044911_CA5C_icechart_dmi_202102080820_CentralEast_RIC.nc',
    
    # 08/02/2021 _2
    #'/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082007_20210208T082107_036496_044911_8C01_icechart_dmi_202102080820_CentralEast_RIC.nc',
    
    # 10/02/2021 _1
    '/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183506_20210210T183610_025548_030B57_6234_icechart_dmi_202102101835_CentralEast_RIC.nc',
    
    # 10/02/2021 _2
    #'/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183610_20210210T183710_025548_030B57_8C33_icechart_dmi_202102101835_CentralEast_RIC.nc',
    
    # 14/02/2021
    #'/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210214T185202_20210214T185306_036590_044C66_CBC6_icechart_dmi_202102141850_CentralEast_RIC.nc'
]

# Create figure
fig, axes = plt.subplots(1, len(files), figsize=(6 * len(files), 6))

if len(files) == 1:
    axes = [axes]

for ax, file in zip(axes, files):

    ds = xr.open_dataset(file)
    data = ds.polygon_icechart.values

    # Get unique polygon IDs (exclude NaN)
    unique_vals = np.unique(data[~np.isnan(data)])

    # Re-index to 0..N-1 for discrete plotting
    indexed = np.full_like(data, fill_value=-1)

    for i, val in enumerate(unique_vals):
        indexed[data == val] = i

    # Create discrete HSV colormap
    cmap = plt.get_cmap("hsv", len(unique_vals))
    cmap.set_under("white")
    # Define discrete boundaries
    norm = BoundaryNorm(
        np.arange(-0.5, len(unique_vals) + 0.5),
        len(unique_vals)
    )

    # Plot
    im = ax.imshow(indexed, cmap=cmap, norm=norm, origin="upper")
    ax.set_title(file.split("/")[-1][:25])
    ax.set_xlabel("sar_samples")
    ax.set_ylabel("sar_lines")

    # Colorbar with actual polygon IDs
    cbar = fig.colorbar(im, ax=ax, ticks=np.arange(len(unique_vals)))
    cbar.ax.set_yticklabels(unique_vals.astype(int))
    cbar.set_label("polygon_id")

plt.tight_layout()
plt.show()



#Now we focus on the SAR image
zips = [
    '/dmidata/projects/asip-cms/sentinel1/2021/02/08/S1A_EW_GRDM_1SDH_20210208T082107_20210208T082207_036496_044911_CA5C.zip',
    '/dmidata/projects/asip-cms/sentinel1/2021/02/10/S1B_EW_GRDM_1SDH_20210210T183506_20210210T183610_025548_030B57_6234.zip'
]

s1p = S1Processor(zips[0])
s1p._transform_gcps(3411)
s1p._set_gcps_to_sea_level()
gcp_grid_shape = (len(s1p.gcps['sample'][s1p.gcps['sample'] == 0]), len(s1p.gcps.line[s1p.gcps.line == 0]))
X, Y = upsample_gcp_grid_RectBiSpl(s1p.gcps.line.values.reshape(gcp_grid_shape),
    s1p.gcps['sample'].values.reshape(gcp_grid_shape),
    s1p.gcps.lon.values.reshape(gcp_grid_shape),
    s1p.gcps.lat.values.reshape(gcp_grid_shape),
    s1p.shape)
HH, HV = s1p.process(calib='sigma', remove_thermal_noise=True)


sar_raw_files = glob.glob('/dmidata/projects/ai4arctic/asidv3/sentinel1_raw/S1A_EW_*202102*')
s1p = S1Processor(sar_raw_files[0])
HH, HV = s1p.process()


plt.figure(figsize=(15, 15))
plt.imshow(HH, vmin=np.nanpercentile(HH, 10), vmax=np.nanpercentile(HH, 90), cmap='gist_gray')





#######New figure
import re


files = [
    '/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082107_20210208T082207_036496_044911_CA5C_icechart_dmi_202102080820_CentralEast_RIC.nc',
    '/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183506_20210210T183610_025548_030B57_6234_icechart_dmi_202102101835_CentralEast_RIC.nc',
]

fig, axes = plt.subplots(1, len(files), figsize=(6 * len(files), 6))

if len(files) == 1:
    axes = [axes]

for i_ax, (ax, file) in enumerate(zip(axes, files)):

    ds = xr.open_dataset(file)
    data = ds.polygon_icechart.values

    # Unique polygon IDs
    unique_vals = np.unique(data[~np.isnan(data)])
    n_colors_needed = len(unique_vals)

    # Re-index
    indexed = np.full_like(data, fill_value=-1)
    for i, val in enumerate(unique_vals):
        indexed[data == val] = i

    # -----------------------------
    # FLIPS
    # -----------------------------
    if i_ax == 0:
        indexed = np.fliplr(indexed)  # izquierda ↔ derecha

    if i_ax == 1:
        indexed = np.flipud(indexed)  # arriba ↕ abajo
        # ⚠️ quitamos la rotación de 90° izquierda

    # -----------------------------
    # Colormap discreto estilo tab20/tab50
    # -----------------------------
    base_cmap = plt.get_cmap("tab20")  # 20 colores base
    colors = list(base_cmap.colors)

    # Si necesitamos más colores, generamos colores extra
    if n_colors_needed > len(colors):
        extra = n_colors_needed - len(colors)
        extra_colors = plt.cm.tab20c(np.linspace(0, 1, extra))
        colors.extend(extra_colors[:, :3])  # quitar alfa

    cmap = ListedColormap(colors[:n_colors_needed])
    cmap.set_under("white")

    norm = BoundaryNorm(
        np.arange(-0.5, len(unique_vals) + 0.5),
        len(unique_vals)
    )

    im = ax.imshow(indexed, cmap=cmap, norm=norm, origin="upper")

    # -----------------------------
    # EXTRAER FECHA Y HORA
    # -----------------------------
    match = re.search(r'_(\d{8})(\d{4})_', file)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        formatted_time = f"{time_str[:2]}:{time_str[2:]}"
        title = f"{formatted_date} {formatted_time} UTC"
    else:
        title = "Unknown date"

    ax.set_title(title, fontsize=16)
    ax.set_xlabel("sar_samples")
    ax.set_ylabel("sar_lines")

    # -----------------------------
    # COLORBAR ABAJO
    # -----------------------------
    cbar = fig.colorbar(
        im,
        ax=ax,
        orientation="horizontal",
        pad=0.08
    )
    cbar.set_ticks(np.arange(len(unique_vals)))
    cbar.set_ticklabels(unique_vals.astype(int))
    cbar.set_label("polygon_id")

plt.tight_layout()
plt.show()

