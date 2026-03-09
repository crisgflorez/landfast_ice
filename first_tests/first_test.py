import os
import glob
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from eoutils import S1Processor
import numpy as np
from matplotlib.colors import BoundaryNorm

# Path to the saved matching files
matching_files_path = 'matching_files.txt'

# Check if the file exists
if os.path.exists(matching_files_path):
    print("Loading matching files from disk...")
    with open(matching_files_path, 'r') as f:
        matching_files = [line.strip() for line in f]
else:
    print("Computing matching files for the first time...")

    # All netCDF files from 2018 to 2021
    # Canadian ice charts
    cis_files = glob.glob('/dmidata/projects/ai4arctic/asidv3/dataset_files/*_EW_*_cis_*.nc')
    # Danish ice charts
    dmi_files = glob.glob('/dmidata/projects/ai4arctic/asidv3/dataset_files/*_EW_*_dmi_*.nc')

    ice_charts_filtered = cis_files + dmi_files

    matching_files = []

    for f in ice_charts_filtered:
        ds = xr.open_dataset(f)
        
        # polygon_codes is a 1D array of strings
        pc = ds.polygon_codes.values
        
        # first row is header
        header = pc[0].split(';')
        
        # rest are data rows
        data_rows = [row.split(';') for row in pc[1:]]
        
        # convert to pandas DataFrame
        df = pd.DataFrame(data_rows, columns=header)
        
        # check if any row matches CT=92 and FA=8
        if ((df['CT'] == '92') & (df['FA'] == '8')).any():
            matching_files.append(f)
        #I did a previous test to ckeck if there was any files in which CT=92 and either FA, FB or FC were =8
        # in all the cases FA was always =8 so the code above is sufficient

    # save to disk for future use
    with open(matching_files_path, 'w') as f:
        for file in matching_files:
            f.write(file + '\n')

print(f"Found {len(matching_files)} matching files.")




# We open one .nc files in which there is fast ice

file_name_nc='/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210323T210934_20210323T211034_037131_045F2E_F55E_icechart_dmi_202103232110_SouthWest_RIC.nc'
ds = xr.open_dataset(file_name_nc)

np.unique(ds.polygon_icechart.values)
#1,3,28 and 54

#Plot to show the polygon ice chart values
plt.imshow(ds.polygon_icechart.values, vmin=0, vmax=60)
plt.colorbar()
plt.title("Polygon Ice Chart IDs")
plt.show()

#I have checked and only poly_id= 1 and 2 contain fast ice
# But if i plot only poly_id=1 we can not see anything 
# because most of the pixels are classified as poly_id = 28 and 54
plt.imshow(ds.polygon_icechart.values==1, vmin=0, vmax=60)
plt.colorbar()
plt.title("Polygon Ice Chart IDs")
plt.show()


#We make a zoom on the area where poly_id=1 is present to see if we can see something in the SAR image
# Mask for class 1
mask = ds.polygon_icechart.values == 1
coords = np.argwhere(mask)

# Get the limits in the original array
min_row, min_col = coords.min(axis=0)
max_row, max_col = coords.max(axis=0)

# Zoom on the mask
zoom_mask = mask[min_row:max_row+1, min_col:max_col+1]

# Display with imshow while keeping SAR grid indices
plt.figure(figsize=(8, 6))
plt.imshow(zoom_mask, cmap="gray", origin="upper")
plt.title("Class 1 on SAR Grid")

# Adjust ticks to show original array coordinates
plt.xticks(
    ticks=[0, zoom_mask.shape[1]-1], 
    labels=[min_col, max_col]
)
plt.yticks(
    ticks=[0, zoom_mask.shape[0]-1], 
    labels=[min_row, max_row]
)

plt.xlabel("Columns (sar_samples)")
plt.ylabel("Rows (sar_lines)")
plt.show()


# Now we do the same but for class 3 and we plot both class 1 and class 3 together to see if we can see something in the SAR image
# Original array
data = ds.polygon_icechart.values

# Mask for class 1 or class 3
mask = (data == 1) | (data == 3)
coords = np.argwhere(mask)

# Get the limits of the zoom rectangle
min_row, min_col = coords.min(axis=0)
max_row, max_col = coords.max(axis=0)

# Zoom on the data
zoom_data = data[min_row:max_row+1, min_col:max_col+1]

# Create a plot array for coloring: 0=background, 1=class1, 2=class3
plot_array = np.zeros_like(zoom_data, dtype=int)
plot_array[zoom_data == 1] = 1
plot_array[zoom_data == 3] = 2

# Define colors: 0=white, 1=red, 2=blue
cmap = ListedColormap(["white", "red", "blue"])
norm = BoundaryNorm([0, 0.5, 1.5, 2.5], cmap.N)

# Create figure and axes
fig, ax = plt.subplots(figsize=(10, 8))

# Display image
im = ax.imshow(plot_array, cmap=cmap, norm=norm, origin="upper")
ax.set_title("Zoom: Icechart Classes 1 (Red) and 3 (Blue)")

# Adjust ticks to reflect original SAR grid indices
ax.set_xticks([0, plot_array.shape[1]-1])
ax.set_xticklabels([min_col, max_col])
ax.set_yticks([0, plot_array.shape[0]-1])
ax.set_yticklabels([min_row, max_row])
ax.set_xlabel("Columns (sar_samples)")
ax.set_ylabel("Rows (sar_lines)")

# Add colorbar directly from the image
cbar = fig.colorbar(im, ax=ax, ticks=[1, 2])
cbar.ax.set_yticklabels(["Class 1", "Class 3"])

plt.show()










sar = '/dmidata/projects/ai4arctic/asidv3/sentinel1_raw/S1A_EW_GRDM_1SDH_20210323T210934_20210323T211034_037131_045F2E_F55E.zip'

s1p = S1Processor(sar)

HH, HV = s1p.process()


plt.figure(figsize=(15, 15))
plt.imshow(HH, vmin=np.nanpercentile(HH, 10), vmax=np.nanpercentile(HH, 90), cmap='gist_gray')






#Second try
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

# 08/02/2021 _1
file_name_nc='/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082107_20210208T082207_036496_044911_CA5C_icechart_dmi_202102080820_CentralEast_RIC.nc'
# 08/02/2021 _2
file_name_nc='/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082007_20210208T082107_036496_044911_8C01_icechart_dmi_202102080820_CentralEast_RIC.nc'

# 10/02/2021_1
file_name_nc='/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183506_20210210T183610_025548_030B57_6234_icechart_dmi_202102101835_CentralEast_RIC.nc'
# 10/02/2021_2
file_name_nc='/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183610_20210210T183710_025548_030B57_8C33_icechart_dmi_202102101835_CentralEast_RIC.nc'

# 14/02/2021
file_name_nc='/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210214T185202_20210214T185306_036590_044C66_CBC6_icechart_dmi_202102141850_CentralEast_RIC.nc'



ds = xr.open_dataset(file_name_nc)

np.unique(ds.polygon_icechart.values)
#1,3,28 and 54

#Plot to show the polygon ice chart values
plt.imshow(ds.polygon_icechart.values, vmin=0, vmax=60)
plt.colorbar()
plt.title("Polygon Ice Chart IDs")
plt.show()


# List of files
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




sar_raw_files = glob.glob('/dmidata/projects/ai4arctic/asidv3/sentinel1_raw/S1A_EW_*202102*')

s1p = S1Processor(sar_raw_files[0])

HH, HV = s1p.process()


plt.figure(figsize=(15, 15))
plt.imshow(HH, vmin=np.nanpercentile(HH, 10), vmax=np.nanpercentile(HH, 90), cmap='gist_gray')















raw_dir = '/dmidata/projects/ai4arctic/asidv3/sentinel1_raw'
raw_files = glob.glob(os.path.join(raw_dir, '*.zip'))

#Build a lookup dictionary for raw files
def extract_key_from_raw(fname):
    base = os.path.basename(fname)
    return '_'.join(base.split('_')[:6])

raw_lookup = {}
for f in raw_files:
    key = extract_key_from_raw(f)
    raw_lookup.setdefault(key, []).append(f)


#Extract the same key from ice chart files    
def extract_key_from_icechart(fname):
    base = os.path.basename(fname)
    return '_'.join(base.split('_')[:6])

# Match ice charts → raw Sentinel-1 files
matched_pairs = []

for ice_file in matching_files:
    key = extract_key_from_icechart(ice_file)
    if key in raw_lookup:
        for raw_file in raw_lookup[key]:
            matched_pairs.append((ice_file, raw_file))

# Saving all SAR images 1 day before and 1 day after ice charts with CT=92 and FA=08

plt.imshow(ds.polygon_icechart.values)

# Open one matched pair
ice_chart= xr.open_dataset(matched_pairs[0][0])
plt.imshow(ice_chart.polygon_icechart.values)

sar=S1Processor(matched_pairs[0][1])
hh,hv=sar.process(bands=['HH','HV'],calib='sigma',remove_thermal_noise=True)
plt.imshow(hh,cmap='gray',vmin=np.nanpercentile(hh,2),vmax=np.nanpercentile(hh,98))


# Open a netCDF file 
ice_charts_1 = xr.open_dataset(ice_charts_filtered[1])

# Check polygon_codes 
# Check polygon_icecharts




# All available raw SAR data
zip_file = os.path.join('/dmidata/projects/ai4arctic/asidv3/sentinel1_raw/', os.path.basename(ncs[n]).split('_icechart_')[0] + '.zip')

