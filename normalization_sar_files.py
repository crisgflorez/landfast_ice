from eoutils import S1Processor, RCMProcessor
import pandas as pd
import os
import numpy as np
import glob
from datetime import datetime
import pyresample
from eoutils import S1Processor
import matplotlib.pyplot as plt
import xarray as xr

def get_processor(sar_file):
    fname = os.path.basename(sar_file)

    if fname.startswith("RCM"):
        return RCMProcessor(sar_file)
    elif fname.startswith("S1"):
        return S1Processor(sar_file)
    else:
        raise ValueError(f"Unknown SAR product type: {fname}")


# =========================
# LOAD DATA
# =========================

df = pd.read_csv("/dmidata/users/cgf/files/selected_sar_files_v1.csv")
#We concatenate the sar and base files to get a list of all files 
sar_files_with_folders = pd.DataFrame({
    "folder": pd.concat([df["folder"], df["folder"]], ignore_index=True),
    "sar_files": pd.concat([df["sar_file"], df["base_file"]], ignore_index=True)
})

# remove duplicates ONLY if both folder + file are identical
sar_files_with_folders = sar_files_with_folders.drop_duplicates(
    subset=["folder", "sar_files"]
).reset_index(drop=True)
# We convert the folder column to datetime and then back to string
# this is the name of the folders in /dmidata/projects/asip-cms/cgf
sar_files_with_folders["folder"] = (
    pd.to_datetime(sar_files_with_folders["folder"])
      .dt.strftime("%Y%m%dT%H%M%S")
)

#We sample 300 files from the list of all files
sampled_files = sar_files_with_folders.sample(n=300, random_state=42)
base_path = "/dmidata/projects/asip-cms/cgf"
S1 = 0.0
S2 = 0.0
n  = 0
for idx, row in sampled_files.iterrows():
    folder = row["folder"]
    sar_file = row["sar_files"]

    print(folder, sar_file)
    path_file = os.path.join(base_path, folder, sar_file)
    if not os.path.exists(path_file):
        print(f"File not found: {path_file}")
        continue

    # We need to open these files and calculate the mean and std of the pixel values in these files to use for normalization
    s1p = get_processor(path_file)
    HH, HV = s1p.process(calib='sigma', remove_thermal_noise=True)

    hh = HH.astype(np.float64)
    hv = HV.astype(np.float64)

    hh = hh[np.isfinite(hh)]
    hv = hv[np.isfinite(hv)]
    x = np.concatenate([hh, hv])
    S1 += x.sum()
    S2 += (x ** 2).sum()
    n  += x.size


#S1=np.float64(-314898167846.3972)
#n=19695930970
#S2=np.float64(6220595060020.863)

mean=S1 / n  #np.float64(-15.987980884276892)
std=np.sqrt((S2 / n) - mean ** 2) #np.float64(7.7598936112507655)

results = pd.DataFrame({
    "mean": [mean],
    "std": [std],
    "n_pixels": [n]
})

results.to_csv("/dmidata/users/cgf/files/normalization_stats_300samples.csv", index=False)

