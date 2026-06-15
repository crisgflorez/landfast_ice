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

if not os.path.exists(checkpoint_path):
    with open(checkpoint_path, "w") as f:
        f.write("last_row_fully_processed\n")
        f.write("0\n")

with open("/dmidata/users/cgf/files/checkpoint.txt", "r") as f:
    lines = f.read().splitlines()
    start_row = int(lines[1]) + (1 if int(lines[1]) != 0 else 0)

for idx_row, row in list_files.iloc[start_row:].iterrows():

    print(f"Processing row {idx_row}") 

    # -------------------------
    # Build paths
    # -------------------------
    zip_1 = os.path.join(base_path, row["folder"], row["sar_file"])
    zip_2 = os.path.join(base_path, row["folder"], row["base_file"])
    nc = row["nc_file_containing_ice_chart"]

    # -------------------------
    # Skip missing files
    # -------------------------
    if not os.path.exists(zip_1):

        pd.DataFrame([{
            "row_idx": idx_row,
            "zip_1": zip_1,
            "zip_2": zip_2,
            "nc": nc,
            "error": "Missing zip_1"
        }]).to_csv(
            exception_csv,
            mode="a",
            header=not os.path.exists(exception_csv),
            index=False
        )

        print(f"Missing zip_1: {zip_1}")
        continue

    if not os.path.exists(zip_2):

        pd.DataFrame([{
            "row_idx": idx_row,
            "zip_1": zip_1,
            "zip_2": zip_2,
            "nc": nc,
            "error": "Missing zip_2"
        }]).to_csv(
            exception_csv,
            mode="a",
            header=not os.path.exists(exception_csv),
            index=False
        )

        print(f"Missing zip_2: {zip_2}")
        continue

    if not os.path.exists(nc):

        pd.DataFrame([{
            "row_idx": idx_row,
            "zip_1": zip_1,
            "zip_2": zip_2,
            "nc": nc,
            "error": "Missing nc"
        }]).to_csv(
            exception_csv,
            mode="a",
            header=not os.path.exists(exception_csv),
            index=False
        )

        print(f"Missing nc: {nc}")
        continue

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


        scene1_patches = patchify(scene1, patch_size=patch_size, step_size=step_size)
        scene2_patches = patchify(scene2, patch_size=patch_size, step_size=step_size)
        ice_chart_patches = patchify(ice_chart, patch_size=patch_size, step_size=step_size)


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

        if len(valid_indices) == 0:

            pd.DataFrame([{
                "row_idx": idx_row,
                "zip_1": zip_1,
                "zip_2": zip_2,
                "nc": nc
            }]).to_csv(
                empty_patches_csv,
                mode="a",
                header=not os.path.exists(empty_patches_csv),
                index=False
            )

            print(f"No valid patches for row {idx_row}")

            continue

        #We only save the patches when there is at least one valid patch in the scene
        zarr_folder = os.path.join(out_dir_zarr_files, row['folder'])
        os.makedirs(zarr_folder, exist_ok=True)

        patch_shape = (5, patch_size, patch_size)
        chunk_shape = (5, patch_size // 2, patch_size // 2)

        patch_dtype = "float16" # I typically use float16 because I use mixed precision training (https://docs.pytorch.org/tutorials/recipes/recipes/amp_recipe.html) when I train models, use float32 if you don't plan to use mixed precision training

        compressor = numcodecs.Blosc(
            cname='lz4hc',
            clevel=5,
            shuffle=numcodecs.Blosc.SHUFFLE
        )

        for idx in tqdm(valid_indices):
            # -------------------------
            # Extract patches
            # -------------------------

            s1 = scene1_patches[idx]      # (2,H,W)
            s2 = scene2_patches[idx]      # (2,H,W)

            ice = ice_chart_patches[idx]

            # add channel dimension to ice
            ice = ice.unsqueeze(0)        # (1,H,W)

            # -------------------------
            # Stack all channels
            # -------------------------

            patch = torch.cat([s2, s1, ice], dim=0)

            # final shape:
            # (5, H, W)

            # -------------------------
            # Create zarr archive
            # -------------------------

            patch_archive = zarr.open(
                os.path.join(zarr_folder, f"patch_{idx}_{row['direction']}.zarr"),
                mode='w',
                shape=patch_shape,
                chunks=chunk_shape,
                dtype=patch_dtype,
                compressor=compressor
            )

            # -------------------------
            # Save patch
            # -------------------------

            patch_archive[:] = patch.half().numpy()

        with open(checkpoint_path, "w") as f:
            f.write("last_row_fully_processed\n")
            f.write(str(idx_row) + "\n") 

    except Exception as e:
        pd.DataFrame([{
            "row_idx": idx_row,
            "zip_1": zip_1,
            "zip_2": zip_2,
            "nc": nc,
            "error": traceback.format_exc()
        }]).to_csv(
            exception_csv,
            mode="a",
            header=not os.path.exists(exception_csv),
            index=False
        )

        print(f"Error processing row {idx_row}")
        print(traceback.format_exc())

        continue
            
    finally:
        try:
            # -------------------------
            # MEMORY CLEANUP 
            # -------------------------

            del scene1, scene2, ice_chart
            del scene1_patches, scene2_patches, ice_chart_patches
            del scene_1_resampled
            del HH_1_norm, HV_1_norm, HH_2_norm, HV_2_norm
            del HH_1_resampled, HV_1_resampled
            del X_1, Y_1, X_2, Y_2
            del ds
        except NameError:
            pass

        gc.collect()