import pandas as pd
import os
from dotenv import load_dotenv
from EODMS import EODMS
from eodms_dds import aaa, dds
import multiprocessing
import time
import shutil

# Load .env for EODMS credentials
load_dotenv()

eodms = EODMS(os.getenv('EODMS_USR'), os.getenv('EODMS_PW'))
eodms_username = os.getenv('EODMS_USR')
eodms_password = os.getenv('EODMS_PW')
rcm_dds_attempts = 5
rcm_download_processes = 6

if not eodms_username or not eodms_password:
    print("EODMS_USR and EODMS_PW environment variables must be set.")
    raise ValueError("Missing EODMS credentials in environment variables.")

try:
    aaa_client = aaa.AAA_API(eodms_username, eodms_password)
    dds_client = dds.DDS_API(aaa_client)
    print('DDS client initialized succesfully.')
except Exception as e:
    print(f"Failed to initialize DDS client: {e}")


base_path = "/dmidata/projects/asip-cms/cgf"
csv_path = "/dmidata/users/cgf/files/matching_results_rcm_final.txt"

# Load the file (adjust path if needed)
df = pd.read_csv(csv_path)

# Filter rows with RCM images
df_filtered = df[
    (df["nb_SAR_images_day_after"] > 0) |
    (df["nb_SAR_images_day_before"] > 0)
]


results = []
print("Number of rows with SAR images before or after:", len(df_filtered))
for _, row in df_filtered.iterrows():
    filename = row["nc_filename"]
    
    # Extract the 5th part (index 4)
    try:
        folder_name = filename.split("_")[4]
    except IndexError:
        print(f"Skipping malformed filename: {filename}")
        continue

    expected_count = row["nb_SAR_images_day_after"] + row["nb_SAR_images_day_before"]
    folder_path = os.path.join(base_path, folder_name)

    if not os.path.isdir(folder_path):
        results.append((filename, folder_name, "FOLDER NOT FOUND", expected_count, 0))
        continue

    # Count files starting with RCMW3
    files = [f for f in os.listdir(folder_path) if f.startswith("RCM")]
    actual_count = len(files)

    status = "OK" if actual_count == expected_count else "MISMATCH"

    results.append((filename, folder_name, status, expected_count, actual_count))


# Print results
for r in results:
    print(r)


mismatch_count = sum(1 for r in results if r[2] == "MISMATCH")
print("Number of MISMATCH rows:", mismatch_count)
results=pd.DataFrame(results)


def extract_ids(s):
    if pd.isna(s) or s == "":
        return []
    return [x.strip() for x in s.split(",")]

def count_rcm_files(folder):
    if not os.path.exists(folder):
        return 0
    return len([f for f in os.listdir(folder) if f.startswith("RCM")])

def file_exists_for_uuid(folder, uuid):
    if not os.path.exists(folder):
        return False
    return any(uuid in fname for fname in os.listdir(folder))


for _, row in df_filtered.iterrows():
    filename = row["nc_filename"]
    folder_name = filename.split("_")[4]
    folder_path = os.path.join(base_path, folder_name)

    expected = row["nb_SAR_images_day_after"] + row["nb_SAR_images_day_before"]
    actual = count_rcm_files(folder_path)

    if actual == expected:
        continue

    print(f"\n⚠️ MISMATCH: {filename}")
    print(f"Expected: {expected} | Found: {actual}")

    # Collect all archiveIds
    ids_after = extract_ids(row["SAR_images_day_after"])
    ids_before = extract_ids(row["SAR_images_day_before"])
    all_ids = set(ids_after + ids_before)

    for uuid in all_ids:

        success = False

        for i in range(rcm_dds_attempts):
            try:
                response = dds_client.get_item("RCMImageProducts", uuid)['code']

                if response == 200:
                    dds_client.download_item(folder_path)
                    success = True
                    break
                else:
                    print(f"Attempt {i+1}/{rcm_dds_attempts} failed for {uuid}: {response}")
                    time.sleep(15)

            except Exception as e:
                print(f"Exception {i+1}/{rcm_dds_attempts} for {uuid}: {e}")
                time.sleep(5)

        if not success:
            print(f"❌ FAILED permanently: {uuid}")




#We want to check if there are folders in the base path that are before 
# 20180626 for which we can not use s1Processor
base_path = "/dmidata/projects/asip-cms/cgf"
cutoff = 20180626

count = 0
folders_before = []

for name in os.listdir(base_path):
    path = os.path.join(base_path, name)

    # solo carpetas
    if not os.path.isdir(path):
        continue

    # validar formato YYYYMMDDTHHMMSS o YYYYMMDD
    date_part = name.split("T")[0]

    if date_part.isdigit() and len(date_part) == 8:
        if int(date_part) < cutoff:
            count += 1
            folders_before.append(name)

print("TOTAL:", count)


base_path = "/dmidata/projects/asip-cms/cgf"
target_path = "/dmidata/projects/asip-cms/cgf/data_before_20180626"

cutoff = 20180626

# create destination folder if it doesn't exist
os.makedirs(target_path, exist_ok=True)

moved = []

for name in os.listdir(base_path):
    src = os.path.join(base_path, name)

    # skip target folder itself (important!)
    if name == "data_before_20180626":
        continue

    date_part = name.split("T")[0]

    if date_part.isdigit() and len(date_part) == 8:
        if int(date_part) < cutoff:
            dst = os.path.join(target_path, name)
            shutil.move(src, dst)
            moved.append(name)

print(f"Moved {len(moved)} folders")