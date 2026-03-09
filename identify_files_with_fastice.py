import os
import glob
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from eoutils import S1Processor
import numpy as np
from matplotlib.colors import BoundaryNorm

# Path to the saved matching files
matching_files_path = '/dmidata/users/cgf/files/files_with_fast_ice_dmi_cis_met.txt'

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
    # Norwegian ice charts
    nor_files = glob.glob('/dmidata/projects/ai4arctic/asidv3/dataset_files/*_EW_*_met_*.nc')
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
        if ((df['CT'] == '92') & (df['FA'].isin(['8', '08']))).any():
            matching_files.append(f)
        #I did a previous test to ckeck if there was any files in which CT=92 and either FA, FB or FC were =8
        # in all the cases FA was always =8 so the code above is sufficient

    # save to disk for future use
    with open(matching_files_path, 'w') as f:
        for file in matching_files:
            f.write(file + '\n')

print(f"Found {len(matching_files)} matching files.")

# Initialize counters
count_dmi = 0
count_cis = 0
count_nor = 0

for f in matching_files:
    if '_dmi_' in f:
        count_dmi += 1
    elif '_cis_' in f:
        count_cis += 1
    elif '_met_' in f:
        count_nor += 1

print(f"DMI files matching criteria: {count_dmi}")
print(f"CIS files matching criteria: {count_cis}")
print(f"NOR files matching criteria: {count_nor}")