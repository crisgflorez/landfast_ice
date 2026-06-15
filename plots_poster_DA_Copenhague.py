from eoutils import S1Processor, RCMProcessor
import pandas as pd
import os
import numpy as np
import glob
from datetime import datetime
import pyresample
import matplotlib.pyplot as plt
import xarray as xr
import torch
from math import ceil
import zarr
import numcodecs
from tqdm import tqdm
import pyproj
from pyproj import Transformer
import re
import gc
import traceback
import cartopy
import geopandas as gpd
from shapely.geometry import shape, Point, MultiPoint, MultiPolygon
import matplotlib as mpl
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
import matplotlib.patches as mpatches

os.environ['HDF5_USE_FILE_LOCKING']='FALSE'

def get_processor(sar_file):
    fname = os.path.basename(sar_file)

    if fname.startswith("RCM"):
        return RCMProcessor(sar_file)
    elif fname.startswith("S1"):
        return S1Processor(sar_file)
    else:
        raise ValueError(f"Unknown SAR product type: {fname}")

# Function that upsamples the GCP coordinates to <new_grid_shape>. Useful when we want a lat/lon coordinate for each pixel in the SAR image
def upsample_gcp_grid_RectBiSpl(lines, samples, x, y, new_grid_shape):
    import numpy as np
    from scipy import interpolate

    upsampled_lines = np.linspace(0, lines.max(), new_grid_shape[0])
    upsampled_samples = np.linspace(0, samples.max(), new_grid_shape[1])

    x_interp = interpolate.RectBivariateSpline(lines[:, 0], samples[0], x)
    y_interp = interpolate.RectBivariateSpline(lines[:, 0], samples[0], y)

    x_up = x_interp(upsampled_lines, upsampled_samples)
    y_up = y_interp(upsampled_lines, upsampled_samples)

    return x_up, y_up

# Function to transform coordinates between different projections (using EPSG codes for the projections, e.g. 4326 for lat/lon, 3411 for North Polar Stereographic proj.)
def transform_points(x, y, fromEPSG, toEPSG):

    transformer = Transformer.from_crs(pyproj.CRS(f'EPSG:{fromEPSG}'), pyproj.CRS(f'EPSG:{toEPSG}'), always_xy=True)
    x, y = transformer.transform(x, y)

    return x, y



# Functions for padding and sliding window ("patchify")
# Tore added an extra stride in the padding
# I changed it and removed the extra stride
def compute_padded_shape(shape, patch_size, step_size):
    if step_size <= patch_size//2:
        print('Step_size must be larger than patch_size//2')
        raise NotImplementedError
    
    n0 = 0
    while n0*step_size + patch_size < shape[0]:
        n0 += 1

    n1 = 0
    while n1*step_size + patch_size < shape[1]:
        n1 += 1

    return ((n0)*step_size + patch_size, (n1)*step_size + patch_size)

def pad(arr, padded_shape, mode='constant', pad_value=255):

    original_2d = False

    if len(arr.shape) == 2:
        original_2d = True
        arr = arr.unsqueeze(dim=0)

    horizontal_pad = padded_shape[1] - arr.shape[2] 
    vertical_pad = padded_shape[0] - arr.shape[1] 
        
    top = ceil(vertical_pad/2)
    bottom = vertical_pad//2
    left = ceil(horizontal_pad/2)
    right = horizontal_pad//2

    arr = torch.nn.functional.pad(
        arr,
        (left, right, top, bottom),
        mode=mode,
        value=pad_value if mode == 'constant' else None
    )
    if original_2d:
        arr = arr.squeeze(0)

    return arr, (top, bottom, left, right)

def patchify(img, patch_size, step_size):
    """
    Patchifies single-channel (HxW) or multi-channel (CxHxW) images represented by tensors.
    Returns a list a ndarray patches.
    """
    
    if len(img.shape) == 2:
        img = img.unsqueeze(dim=0)
    
    patches = []
    for i in range(int(img.shape[-2]/step_size)):
        for j in range(int(img.shape[-1]/step_size)):
            patch = img[:, i*step_size:i*step_size + patch_size, j*step_size:j*step_size + patch_size]
            patches.append(patch.squeeze())
            
    return patches

def patchify_with_coords(img, x, y, patch_size, step_size):
    patches = []
    coords = []

    is_2d = (len(img.shape) == 2)

    for i in range(int(img.shape[-2] / step_size)):
        for j in range(int(img.shape[-1] / step_size)):

            y0 = i * step_size
            y1 = y0 + patch_size
            x0 = j * step_size
            x1 = x0 + patch_size

            if is_2d:
                patch = img[y0:y1, x0:x1]
            else:
                patch = img[:, y0:y1, x0:x1]

            x_patch = x[y0:y1, x0:x1]
            y_patch = y[y0:y1, x0:x1]

            patches.append(patch)
            coords.append((x_patch, y_patch))

    return patches, coords


#Load the mean and std values for the HH and HV bands
mean_std_df = pd.read_csv("/dmidata/users/cgf/files/normalization_stats_300samples.csv")
mean_HH_HV=mean_std_df['mean'].values
std_HH_HV=mean_std_df['std'].values

#List of files that we are going to use to train/test our model.
list_files = pd.read_csv("/dmidata/users/cgf/files/selected_sar_files_v1.csv")
list_files["folder"] = pd.to_datetime(list_files["folder"])
list_files["folder"] = list_files["folder"].dt.strftime("%Y%m%dT%H%M%S")
base_path = "/dmidata/projects/asip-cms/cgf"
out_dir_zarr_files = "/dmidata/projects/asip-cms/cgf/zarr_files2"
checkpoint_path = "/dmidata/users/cgf/files/checkpoint.txt"
error_csv = "/dmidata/users/cgf/files/mismatch_shape_sar_and_nc2.csv"
exception_csv = "/dmidata/users/cgf/files/processing_errors.csv"
empty_patches_csv = "/dmidata/users/cgf/files/no_valid_patches.csv"

# -------------------------
# SHAPEFILE
# -------------------------
gdf = gpd.read_file('/dmidata/projects/asip-cms/code/asip_opr/l2_prod/sar_preproc/arctic_shp/op_str_maps_circum_polar_40_EPSG3411.shp')

start_row=650
for idx_row, row in list_files.iloc[start_row:start_row+1].iterrows():

    print(f"Processing row {idx_row}") 

    # -------------------------
    # Build paths
    # -------------------------
    zip_1 = os.path.join(base_path, row["folder"], row["sar_file"])
    zip_2 = os.path.join(base_path, row["folder"], row["base_file"])
    nc = row["nc_file_containing_ice_chart"]

    try:
        # We need the coordinates and HH/HV bands from each scene.
        s1p = get_processor(zip_1)

        # in-built function in the S1Processor class to transform the GCPs between projections. 
        # In the original product the GCPs are given in lat/lon. 
        # We transform to 3411 because we need a planar projection to set the GCPs to sea level and to upsample the GCP grid accurately (too much distortion in lat/lon). 
        s1p._transform_gcps(3411)  
        s1p._set_gcps_to_sea_level()

        # Here, we count the number of "lines" and "samples" (i.e. rows and columns) in the GCP grid to get the GCP grid shape.
        gcp_grid_shape = (len(s1p.gcps['sample'][s1p.gcps['sample'] == 0]), len(s1p.gcps.line[s1p.gcps.line == 0]))

        # We use the helper function to upsample the GCP grid (now in 3411 projection), so we have a coordinate for each pixel in the SAR image.
        X_1, Y_1 = upsample_gcp_grid_RectBiSpl(s1p.gcps.line.values.reshape(gcp_grid_shape),
            s1p.gcps['sample'].values.reshape(gcp_grid_shape),
            s1p.gcps.lon.values.reshape(gcp_grid_shape),
            s1p.gcps.lat.values.reshape(gcp_grid_shape),
            s1p.shape)

        # Calibration and thermal noise removal
        HH_1, HV_1 = s1p.process(calib='sigma', remove_thermal_noise=True)

        HH_1_norm=(HH_1-mean_HH_HV)/std_HH_HV
        HV_1_norm=(HV_1-mean_HH_HV)/std_HH_HV

        # We do this for both scenes
        s1p = get_processor(zip_2)
        s1p._transform_gcps(3411)  
        s1p._set_gcps_to_sea_level()
        gcp_grid_shape = (len(s1p.gcps['sample'][s1p.gcps['sample'] == 0]), len(s1p.gcps.line[s1p.gcps.line == 0]))
        X_2, Y_2 = upsample_gcp_grid_RectBiSpl(s1p.gcps.line.values.reshape(gcp_grid_shape),
            s1p.gcps['sample'].values.reshape(gcp_grid_shape),
            s1p.gcps.lon.values.reshape(gcp_grid_shape),
            s1p.gcps.lat.values.reshape(gcp_grid_shape),
            s1p.shape)

        HH_2, HV_2 = s1p.process(calib='sigma', remove_thermal_noise=True)

        HH_2_norm=(HH_2-mean_HH_HV)/std_HH_HV
        HV_2_norm=(HV_2-mean_HH_HV)/std_HH_HV


        # Now we have the upsampled coordinates for both scenes. 
        # Unfortunately, pyresample need lat/lon to do the resampling, so we need to project back from 3411 to 4326, and that takes some compute time...
        # EPSG:4326 is the standard lat/lon projection, and EPSG:3411 is the North Polar Stereographic projection used for the SAR products.
        lon_1, lat_1 = transform_points(X_1, Y_1, fromEPSG=3411, toEPSG=4326)
        lon_2, lat_2 = transform_points(X_2, Y_2, fromEPSG=3411, toEPSG=4326)

        # Pyresample relates the two scenes geographically using swath definitions
        swath_def_1 = pyresample.SwathDefinition(lon_1, lat_1)
        swath_def_2 = pyresample.SwathDefinition(lon_2, lat_2)

        # Resampling using nearest neighbour..
        # To resample multiple bands at the same time, we have to stack like HxWxC, with C being the bands.
        # Below we resample both bands from scene 1 to the grid of scene 2. 
        scene_1_resampled = pyresample.kd_tree.resample_nearest(
            swath_def_1, # source grid
            np.stack([HH_1_norm, HV_1_norm], axis=2), # source data
            swath_def_2, # target grid
            radius_of_influence=100, # 100 m
            fill_value=np.nan)

        HH_1_resampled = scene_1_resampled[:, :, 0]
        HV_1_resampled = scene_1_resampled[:, :, 1]

        #Open ice chart 
        ds = xr.open_dataset(nc)

        # Convert directly to DataFrame by splitting on ';'
        df = pd.Series(ds.polygon_codes.values).str.split(';', expand=True)
        # First row contains column names
        df.columns = df.iloc[0]
        # Drop the first row (header)
        df = df.iloc[1:].reset_index(drop=True)
        # Convert relevant columns to integers
        df[['poly_id', 'CT', 'FA']] = df[['poly_id', 'CT', 'FA']].astype(int)
        # Filter fast ice (CT = 92 and FA = 8)
        result = df[(df['CT'] == 92) & (df['FA'] == 8)]
        # Extract poly_id values
        poly_ids = result['poly_id'].to_list()

        # Create a mask for the ice chart where pixels that fall within the fast ice polygons are marked as 1, and others as 0
        mask = np.where(
            np.isnan(ds.polygon_icechart.values),
            np.nan,
            np.isin(ds.polygon_icechart.values, poly_ids).astype(float)
        )

        # Check dimensions match
        if mask.shape != HH_2_norm.shape:

            error_row = pd.DataFrame([{
                "row_idx": idx_row,
                "zip_2": zip_2,
                "HH_2_shape": str(HH_2_norm.shape),
                "nc": nc,
                "sar_primary_shape": str(ds.sar_primary.shape)
            }])

            error_row.to_csv(
                error_csv,
                mode="a",
                header=not os.path.exists(error_csv),
                index=False
            )

            continue


        valid_SAR2 = np.isfinite(HH_2_norm)
        valid_SAR1 = np.isfinite(HH_1_resampled)
        valid_ice = np.isfinite(mask)
        global_valid_mask = valid_SAR1 & valid_SAR2 & valid_ice

        # We apply this global valid mask to both scenes and the ice chart, so we have NaNs in the same positions in all arrays, which is important for training the model later on.
        HH_2_clean = np.where(global_valid_mask, HH_2_norm, np.nan)
        HV_2_clean = np.where(global_valid_mask, HV_2_norm, np.nan)
        HH_1_clean = np.where(global_valid_mask, HH_1_resampled, np.nan)
        HV_1_clean = np.where(global_valid_mask, HV_1_resampled, np.nan)

        # We also apply the global valid mask to the ice chart, so we have NaNs in the same positions in all arrays, which is important for training the model later on.
        mask_clean  = np.where(global_valid_mask, mask, np.nan)


        shape = HH_1_clean.shape
        patch_size = 1024
        overlap = 0.25 # 25% overlap between windows
        step_size = int(patch_size*(1-overlap))
        scene1 = torch.from_numpy(np.stack([HH_1_clean, HV_1_clean], axis=0)).float()
        scene2 = torch.from_numpy(np.stack([HH_2_clean, HV_2_clean], axis=0)).float()
        ice_chart = torch.from_numpy(mask_clean).float()

        padded_shape = compute_padded_shape(shape=shape, patch_size=patch_size, step_size=step_size)
        scene1, pad_info1 = pad(scene1, padded_shape=padded_shape, pad_value=np.nan)
        scene2, pad_info2 = pad(scene2, padded_shape=padded_shape, pad_value=np.nan)
        ice_chart, pad_info_mask = pad(
            ice_chart,padded_shape=padded_shape,pad_value=np.nan)


        #scene1_patches = patchify(scene1, patch_size=patch_size, step_size=step_size)
        #scene2_patches = patchify(scene2, patch_size=patch_size, step_size=step_size)
        #ice_chart_patches = patchify(ice_chart, patch_size=patch_size, step_size=step_size)
        scene1_patches, scene1_coords = patchify_with_coords(
            scene1,
            X_2,
            Y_2,
            patch_size,
            step_size
        )

        scene2_patches, scene2_coords = patchify_with_coords(
            scene2,
            X_2,
            Y_2,
            patch_size,
            step_size
        )

        ice_chart_patches, ice_coords = patchify_with_coords(
            ice_chart,
            X_2,   # normalmente el grid de referencia es scene2
            Y_2,
            patch_size,
            step_size
        )

        # Identify valid patches based on the presence of fast ice and the amount of valid data (i.e. non-NaN pixels) in the patch.
        valid_indices = []
        for i in range(len(ice_chart_patches)):

            ice_patch = ice_chart_patches[i].float()
            valid_mask = torch.isfinite(ice_patch)

            #Presence of NaNs
            nan_ratio = torch.isnan(ice_patch).float().mean().item()
            #Presence of fast ice (1 in the ice chart)
            fast_ice_ratio = (
                ((ice_patch == 1) & valid_mask).float().sum()
                / valid_mask.float().sum()
            ).item()
            #Combined filter: we want to keep patches with less than 50% NaNs and at least 5% of the valid pixels in the patch are fast ice.
            if (nan_ratio <= 0.5) and (fast_ice_ratio >= 0.05):
                valid_indices.append(i)
    except Exception as e:


        print(f"Error processing row {idx_row}")
        print(traceback.format_exc())

        continue



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
    

extent = (
    np.nanmin([X_1.min(), X_2.min()]),  # xmin
    np.nanmax([X_1.max(), X_2.max()]),  # xmax
    np.nanmin([Y_1.min(), Y_2.min()]),  # ymin
    np.nanmax([Y_1.max(), Y_2.max()])   # ymax
)

vmin = np.nanpercentile(
    np.concatenate([HH_1_norm.flatten(), HH_2_norm.flatten()]),
    2
)

vmax = np.nanpercentile(
    np.concatenate([HH_1_norm.flatten(), HH_2_norm.flatten()]),
    98
)



#In the netcdf file the coordinates of the CGPs are saved in 
#ds.sar_grid_latitude and ds.sar_grid_longitude
#These values have been already corrected to be at sea level and are in EPSG:4326 (lat/lon), so we need to transform them to 3411 and upsample the GCP grid to have a coordinate for each pixel in the SAR image, which is needed for the resampling and for plotting later on.
# We transform to 3411 because we need a planar projection to set the GCPs to sea level and to upsample the GCP grid accurately (too much distortion in lat/lon). 
lon, lat = transform_points(ds.sar_grid_longitude.values, ds.sar_grid_latitude.values, fromEPSG=4326, toEPSG=3411)
n_lines = len(np.unique(ds.sar_grid_line.values))
n_samples = len(np.unique(ds.sar_grid_sample.values))

gcp_grid_shape = (n_lines, n_samples)
X, Y = upsample_gcp_grid_RectBiSpl(
    ds.sar_grid_line.values.reshape(gcp_grid_shape),
    ds.sar_grid_sample.values.reshape(gcp_grid_shape),
    lon.reshape(gcp_grid_shape),
    lat.reshape(gcp_grid_shape),
    ds.sar_primary.shape
)

fig, ax = plt.subplots(
    1, 3,
    figsize=(16, 16),
    subplot_kw={'projection': NorthPolStere()}
)

# =========================
# PLOT 1
# =========================
ax[0].set_facecolor('#6baed6')
ax[0].set_extent(extent, crs=NorthPolStere())
ax[0].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

skip = 4
mesh1 = ax[0].pcolormesh(
    X_2[::skip, ::skip],
    Y_2[::skip, ::skip],
    HH_2_norm[::skip, ::skip],
    cmap='gray',
    vmin=vmin,
    vmax=vmax,
    transform=NorthPolStere(),
    shading='auto',
    zorder=50
)

# Here we get the extent of the first plot, which we will use for the other two plots to make sure they are all zoomed in on the same area.
extent = ax[0].get_extent()

# =========================
# PLOT 2 
# =========================
ax[1].set_facecolor('#6baed6')

ax[1].set_extent(extent, crs=NorthPolStere())

ax[1].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

mesh2 = ax[1].pcolormesh(
    X_1[::skip, ::skip],
    Y_1[::skip, ::skip],
    HH_1_norm[::skip, ::skip],
    cmap='gray',
    vmin=vmin,
    vmax=vmax,
    transform=NorthPolStere(),
    shading='auto',
    zorder=50
)

# =========================
# FAST ICE MASK
# =========================
ax[2].set_facecolor('#6baed6')
ax[2].set_extent(extent, crs=NorthPolStere())

# -------------------------
# LAND / COASTLINES (shapefile)
# -------------------------
ax[2].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

# -------------------------
# ICE CHART (base categorical field)
# -------------------------
poly_plot = ds.polygon_icechart.values.astype(float)
poly_plot[np.isnan(poly_plot)] = np.nan
base = plt.cm.Greys
cmap = mcolors.LinearSegmentedColormap.from_list(
    "Greys_light",
    base(np.linspace(0.2, 0.8, 256))
)
#cmap.set_bad('red')
mesh = ax[2].pcolormesh(
    X[::skip, ::skip],
    Y[::skip, ::skip],
    poly_plot[::skip, ::skip],
    cmap=cmap,              
    transform=NorthPolStere(),
    shading='auto',
    zorder=20
)
cbar = fig.colorbar(mesh, ax=ax[2], shrink=0.6, pad=0.02)
cbar.set_label("Ice chart polygon ID")
# -------------------------
# FAST ICE MASK (hatch overlay)
# -------------------------
fastice_mask = np.isin(poly_plot, poly_ids).astype(float)
fastice_mask = np.where(fastice_mask == 1, 1, np.nan)

cs = ax[2].contourf(
    X,
    Y,
    fastice_mask,
    levels=[0.5, 1.5],
    colors='none',
    hatches=['//////'],
    transform=NorthPolStere(),
    zorder=50
)

# 🔥 TRANSPARENCIA REAL DEL HATCH
for c in cs.collections:
    c.set_edgecolor(mpl.colors.to_rgba('magenta', 0.4))  # alpha aquí
    c.set_alpha(0.4)

legend_elements = [
    Patch(
        facecolor='none',
        edgecolor='magenta',
        hatch='//////',
        label='Fast ice polygons'
    )
]

ax[2].legend(handles=legend_elements, loc='lower left')
ax[2].set_title("Ice chart + Fast ice overlay")

fig.savefig("/dmidata/users/cgf/plots/ice_sar_plot"+str("_".join(os.path.basename(zip_1).split("_")[:-1]))+".png", dpi=300, bbox_inches="tight")




#Plot starting point
extent = (X_2.min(), X_2.max(), Y_2.min(), Y_2.max())   

fig, ax = plt.subplots(
    1, 2,
    figsize=(16, 16),
    subplot_kw={'projection': NorthPolStere()}
)

# =========================
# PLOT 1
# =========================
ax[0].set_facecolor('#6baed6')
ax[0].set_extent(extent, crs=NorthPolStere())
ax[0].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

skip = 4 
mesh1 = ax[0].pcolormesh(
    X_2[::skip, ::skip],
    Y_2[::skip, ::skip],
    HH_2_norm[::skip, ::skip],
    cmap='gray',
    vmin=np.nanpercentile(
    HH_2_norm.flatten(),
    2),
    vmax=np.nanpercentile(
    HH_2_norm.flatten(),
    98),
    transform=NorthPolStere(),
    shading='auto',
    zorder=50
)

# Here we get the extent of the first plot, which we will use for the other two plots to make sure they are all zoomed in on the same area.
extent = ax[0].get_extent()

# =========================
# FAST ICE MASK
# =========================
ax[1].set_facecolor('#6baed6')
ax[1].set_extent(extent, crs=NorthPolStere())

# -------------------------
# LAND / COASTLINES (shapefile)
# -------------------------
ax[1].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

# -------------------------
# ICE CHART (base categorical field)
# -------------------------
poly_plot = ds.polygon_icechart.values.astype(float)
poly_plot[np.isnan(poly_plot)] = np.nan
base = plt.cm.Greys
cmap = mcolors.LinearSegmentedColormap.from_list(
    "Greys_light",
    base(np.linspace(0.2, 0.8, 256))
)
#cmap.set_bad('red')
mesh = ax[1].pcolormesh(
    X[::skip, ::skip],
    Y[::skip, ::skip],
    poly_plot[::skip, ::skip],
    cmap=cmap,              
    transform=NorthPolStere(),
    shading='auto',
    zorder=20
)
# -------------------------
# FAST ICE MASK (hatch overlay)
# -------------------------
fastice_mask = np.isin(poly_plot, poly_ids).astype(float)
fastice_mask = np.where(fastice_mask == 1, 1, np.nan)

cs = ax[1].contourf(
    X,
    Y,
    fastice_mask,
    levels=[0.5, 1.5],
    colors='none',
    hatches=['//////'],
    transform=NorthPolStere(),
    zorder=50
)

# 🔥 TRANSPARENCIA REAL DEL HATCH
for c in cs.collections:
    c.set_edgecolor(mpl.colors.to_rgba('magenta', 0.4))  # alpha aquí
    c.set_alpha(0.4)
    
legend_elements = [
    Patch(
        facecolor='none',
        edgecolor='magenta',
        hatch='//////',
        label='Fast ice polygons'
    )
]

ax[1].set_title("Ice chart + Fast ice overlay")
fig.savefig("/dmidata/users/cgf/plots/ice_sar_plot"+str("_".join(os.path.basename(zip_2).split("_")[:-1]))+"collocated_SAR_ice_chart.png", dpi=300, bbox_inches="tight")


###############################
#PLOT 1 of how to build dataset
##############################
fig, ax = plt.subplots(
    1, 3,
    figsize=(16, 16),
    subplot_kw={'projection': NorthPolStere()}
)

# =========================
# PLOT 1
# =========================
ax[0].set_facecolor('#6baed6')
ax[0].set_extent(extent, crs=NorthPolStere())
ax[0].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

skip = 4
mesh1 = ax[0].pcolormesh(
    X_2[::skip, ::skip],
    Y_2[::skip, ::skip],
    HH_2_norm[::skip, ::skip],
    cmap='gray',
    vmin=vmin,
    vmax=vmax,
    transform=NorthPolStere(),
    shading='auto',
    zorder=50
)

# Here we get the extent of the first plot, which we will use for the other two plots to make sure they are all zoomed in on the same area.
extent = ax[0].get_extent()

# =========================
# PLOT 2 
# =========================
ax[1].set_facecolor('#6baed6')

ax[1].set_extent(extent, crs=NorthPolStere())

ax[1].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

mesh2 = ax[1].pcolormesh(
    X_2[::skip, ::skip],
    Y_2[::skip, ::skip],
    HH_1_resampled[::skip, ::skip],
    cmap='gray',
    vmin=vmin,
    vmax=vmax,
    transform=NorthPolStere(),
    shading='auto',
    zorder=50
)

# =========================
# FAST ICE MASK
# =========================
ax[2].set_facecolor('#6baed6')
ax[2].set_extent(extent, crs=NorthPolStere())

# -------------------------
# LAND / COASTLINES (shapefile)
# -------------------------
ax[2].add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

# -------------------------
# ICE CHART (base categorical field)
# -------------------------
from matplotlib.colors import ListedColormap

cmap = ListedColormap([
    'black',    # 0
    'magenta'   # 1
])
#cmap.set_bad('red')
mesh = ax[2].pcolormesh(
    X[::skip, ::skip],
    Y[::skip, ::skip],
    mask[::skip, ::skip],
    cmap=cmap,
    vmin=0,
    vmax=1,
    transform=NorthPolStere(),
    shading='auto',
    zorder=20
)


##Plot sliding windows with valid patches
n=7
fig, ax = plt.subplots(1, n, figsize=(4*n, 6))

for idx, patch_id in enumerate(valid_indices):

    patch = scene1_patches[patch_id][0].cpu().numpy()

    ax[idx].set_title(f"{patch_id}")
    cmap = mpl.cm.gray.copy()
    cmap.set_bad('red')
    ax[idx].imshow(
        patch,
        cmap=cmap,
        vmin=np.nanpercentile(patch, 2),
        vmax=np.nanpercentile(patch, 98)
    )

    ax[idx].axis("off")

plt.tight_layout()
plt.show()


fig, ax = plt.subplots(
    1, 1,
    figsize=(10, 10),
    subplot_kw={'projection': NorthPolStere()}
)

ax.set_facecolor('#6baed6')
ax.set_extent(extent, crs=NorthPolStere())

ax.add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='#6B4F3A',
    edgecolor='black',
    linewidth=0.1,
    zorder=10
)

# SAR base
ax.pcolormesh(
    X_2[::skip, ::skip],
    Y_2[::skip, ::skip],
    HH_1_clean[::skip, ::skip],
    cmap='gray',
    vmin=vmin,
    vmax=vmax,
    transform=NorthPolStere(),
    shading='auto',
    zorder=20
)

# -------------------------
# PATCH OVERLAY (MAGENTA BOXES)
# -------------------------
for patch_id in valid_indices:

    x_patch, y_patch = scene2_coords[patch_id]

    xmin = np.nanmin(x_patch)
    xmax = np.nanmax(x_patch)
    ymin = np.nanmin(y_patch)
    ymax = np.nanmax(y_patch)

    rect = mpatches.Rectangle(
        (xmin, ymin),
        xmax - xmin,
        ymax - ymin,
        facecolor='none',
        edgecolor='green',
        linewidth=2,
        transform=NorthPolStere(),
        zorder=100
    )

    ax.add_patch(rect)

plt.show()