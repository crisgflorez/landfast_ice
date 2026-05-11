import os
from CDS import CDS
from datetime import datetime, timedelta
from shapely.geometry import Polygon
from dotenv import load_dotenv
from eoutils import S1Processor
from EODMS import EODMS
import json
import pandas as pd
from shapely.geometry import shape, Point, MultiPoint, MultiPolygon
from shapely.ops import transform
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
from eodms_dds import aaa, dds
import multiprocessing
import time


os.environ['HDF5_USE_FILE_LOCKING']='FALSE'
# Load .env for EODMS credentials
load_dotenv()
cds = CDS(os.getenv('CDS_USR'), os.getenv('CDS_PW'))
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



def get_timestamp(file):

    pattern = re.compile(r"(\d{4}\d{2}\d{2}[_T]\d{2}\d{2}\d{2})") # e.g. 20251023T102620 (S1), 20251008_225228 (RCM)
    # For Sentinel-1, this script graps the first timestamp (the start time)

    match = re.search(pattern, os.path.basename(file))
    if match:
        matched_str = match.group(1)
        timestamp_str = matched_str.replace("_", "").replace("T", "")
        return dateutil.parser.parse(timestamp_str)
    else:
        raise TypeError(f"The following files has no matching timestamp: {file}")
    
def reproject_geometry(geom, fromEPSG=4326, toEPSG=3411):

    transformer = Transformer.from_crs(fromEPSG, toEPSG, always_xy=True)

    return transform(transformer.transform, geom)

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
    

class SouthPolStere(cartopy.crs.Projection):
    """
    A class for the Southern Hemisphere polar stereographic projection.

    This projection is based on EPSG:3976 (WGS 84 / NSIDC Sea Ice
    Polar Stereographic South).
    """
    def __init__(self):
        # See: https://epsg.io/3976
        proj4_params = {
            'proj': 'stere',
            'lat_0': -90.,
            'lon_0': 0,
            'lat_ts': -70,
            'x_0': 0,
            'y_0': 0,
            'a': 6378137,
            'rf': 298.257223563, # WGS84 inverse flattening
            'units': 'm',
            'datum': 'WGS84',
            'no_defs': ''
        }

        super(SouthPolStere, self).__init__(proj4_params)

    @property
    def boundary(self):
        """Returns the rectangular boundary of the projection."""
        coords = ((self.x_limits[0], self.y_limits[0]),
                  (self.x_limits[1], self.y_limits[0]),
                  (self.x_limits[1], self.y_limits[1]),
                  (self.x_limits[0], self.y_limits[1]),
                  (self.x_limits[0], self.y_limits[0]))

        return cartopy.crs.sgeom.Polygon(coords).exterior

    @property
    def threshold(self):
        """The resolution threshold for the projection (in meters)."""
        return 1e5

    @property
    def x_limits(self):
        """The x-axis limits for the projection (in meters)."""
        return (-4200000, 4200000)

    @property
    def y_limits(self):
        """The y-axis limits for the projection (in meters)."""
        return (-4200000, 4200000)
    
class DualPol2RGB():
    def __init__(self):
        #self.config = config
        self.band1_min_val = -28
        self.band2_min_val = -26
        self.band3_min_val = 0

        self.band1_max_val = -1
        self.band2_max_val = -17
        self.band3_max_val = 4
        
        #self.no_data_value = self.config['processing']['no_data_value']

    def _stretch_to_uint8(self, band, min_val, max_val):

        stretched_band = (band - min_val) / (max_val - min_val)
        stretched_band[stretched_band < 0] = 0
        stretched_band[stretched_band > 1] = 1
        return  stretched_band #(stretched_band * 255).astype(np.uint8)
    
    def create_rgb(self, band1, band2, band3):
        
        band1_uint8 = self._stretch_to_uint8(band1, min_val=self.band1_min_val, max_val=self.band1_max_val)
        band2_uint8 = self._stretch_to_uint8(band2, min_val=self.band2_min_val, max_val=self.band2_max_val)
        band3_uint8 = self._stretch_to_uint8(band3, min_val=self.band3_min_val, max_val=self.band3_max_val)
        rgb = np.stack([band1_uint8, band2_uint8, band3_uint8], axis=2)
        #rgb = np.clip(rgb, 0, 254) # clipping at 254 as 255 is used for invalid pixels
        #rgb[np.isnan(band1), :] = self.no_data_value

        return rgb
    
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


def _download_helper(uuid, output_dir):

    for i in range(rcm_dds_attempts):
        response = dds_client.get_item("RCMImageProducts", uuid)['code']

        if response == 200:
            dds_client.download_item(output_dir)
            break
        else:
            print(f"Attempt failed with code {response}. Retrying... {i+1}/{rcm_dds_attempts}")
            time.sleep(10) 




output_path = "/dmidata/users/cgf/files/matching_results_rcm_final.txt"
log_path = "/dmidata/users/cgf/files/invalid_polygons_rcm_final.txt"
total_files = sum(1 for _ in open("/dmidata/users/cgf/files/files_with_fast_ice_dmi_cis_met.txt"))
with open('/dmidata/users/cgf/files/eastern_arctic_polygon.json', 'r') as f:
    eastern_arctic_geojson = json.load(f)

with open('/dmidata/users/cgf/files/western_arctic_polygon.json', 'r') as f:
    western_arctic_geojson = json.load(f)

if os.path.exists(output_path):
    done_files = pd.read_csv(output_path)["nc_filename"].tolist()
    file_exists = True
else:
    done_files = []
    file_exists = False

features = [
    ('intersects', eastern_arctic_geojson),
    ('intersects', western_arctic_geojson),
]

filters = {'Beam Mnemonic': ('LIKE', ['%SCLN%']),#, '%SC50M%', '%SC100M%']),
            'Polarization': ('=', 'HH HV')}


with open("/dmidata/users/cgf/files/files_with_fast_ice_dmi_cis_met.txt", "r") as f, \
     open(log_path, "a") as log_file:  # <- open log file in append mode safely
    
    for idx, line in enumerate(f):

        nc_file = line.strip() 
        filename = os.path.basename(nc_file)

        if filename in done_files:
            print(f"Skipping {filename} (already processed)")
            continue

        # We skip files before 2020/01/01 because there was no RCM data available
        date_file_name = re.search(r'\d{8}', filename).group(0)
        # convert to datetime
        date_file_name = datetime.strptime(date_file_name, "%Y%m%d")

        if date_file_name < datetime(2020, 1, 1):
            print(f"Skipping {filename} (before 2020-01-01)")
            row = pd.DataFrame([{
                "nc_filename": filename,
                "SAR_images_day_after": "",
                "nb_SAR_images_day_after": 0,
                "SAR_images_day_before": "",
                "nb_SAR_images_day_before": 0
            }])
            row.to_csv(
                output_path,
                mode='a',
                header=not file_exists,
                index=False
            )
            file_exists = True            
            continue
                
        #Open a netCDF file containing ice chart data with fast ice, and read the GeoFootprint column to get the coordinates of the area covered by the ice chart. The GeoFootprint column contains a string representation of a polygon in WKT format, which can be parsed using the shapely library to create a Polygon object representing the area covered by the ice chart.
        ds = xr.open_dataset(nc_file) 
        #In the netcdf file the coordinates of the CGPs are saved in 
        #ds.sar_grid_latitude and ds.sar_grid_longitude
        #These values have been already corrected to be at sea level
        lats = ds.sar_grid_latitude.values
        lons = ds.sar_grid_longitude.values
        points = [Point(lon, lat) for lon, lat in zip(lons, lats)]
        footprint = MultiPoint(points).convex_hull
        nc_dts = get_timestamp(os.path.basename(nc_file))
        nc_footprint=reproject_geometry(footprint, fromEPSG=4326, toEPSG=3411)

        n_lines = len(np.unique(ds.sar_grid_line.values))
        n_samples = len(np.unique(ds.sar_grid_sample.values))

        gcp_grid_shape = (n_lines, n_samples)
        X, Y = upsample_gcp_grid_RectBiSpl(
            ds.sar_grid_line.values.reshape(gcp_grid_shape),
            ds.sar_grid_sample.values.reshape(gcp_grid_shape),
            ds.sar_grid_longitude.values.reshape(gcp_grid_shape),
            ds.sar_grid_latitude.values.reshape(gcp_grid_shape),
            ds.sar_primary.shape
        )

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


        polygons = []
        ice_array = ds['polygon_icechart'].values

        for pid in poly_ids:
            # Gwr
            ind = np.where(ice_array == pid)
            
            # Extract the corresponding latitudes and longitudes for the current polygon
            lats_poly = Y[ind]
            lons_poly = X[ind]
            
            #Less than 3 points cannot form a polygon, so we skip those cases and log them for further inspection
            if len(lats_poly) < 3:
                log_file.write(f"{filename}, poly_id={pid}, n_points={len(lats_poly)}\n")
                continue            
            # Create a polygon from the edges (convex hull or alpha shape per polygon)
            poly = MultiPoint(np.column_stack((lons_poly, lats_poly))).convex_hull

            if poly.geom_type != "Polygon":
                log_file.write(f"{filename}, poly_id={pid}, geom_type={poly.geom_type}\n")
                continue            
            polygons.append(poly)

        nc_footprint = MultiPolygon(polygons)
        nc_footprint = reproject_geometry(nc_footprint, fromEPSG=4326, toEPSG=3411)

        #Open SAR file associated to netcdf file
        #zip_file = '/dmidata/projects/asip-cms/sentinel1/2019/08/11/S1B_EW_GRDM_1SDH_20190811T123356_20190811T123456_017538_020FC3_8562.zip'
        #s1p = S1Processor(_zip=zip_file)
        #s1p._transform_gcps(3411)
        #s1p._set_gcps_to_sea_level()
        #s1p._transform_gcps(4326)
        #gcp_grid_shape = (len(s1p.gcps['sample'][s1p.gcps['sample'] == 0]), len(s1p.gcps.line[s1p.gcps.line == 0]))
        #Estimation of the footprint of SAR image based on the GCPs, by creating a convex hull around the GCPs. The convex hull is the smallest convex polygon that can enclose all the GCPs, and it can be used as an approximation of the footprint of the SAR image.
        #s1p._get_gcps_as_geopandas_df().union_all().convex_hull


        # Define your area north of 50 degrees latitude
        coordinates = [(-180, 50), (180, 50), (180, 90), (-180, 90), (-180, 50)]
        north_of_50 = Polygon(coordinates)

        filename = os.path.basename(nc_file)

        # Extract date from filename
        date_part = filename.split('_')[4]  # 'YYYYMMDDThhmmss'
        year = int(date_part[0:4])
        month = int(date_part[4:6])
        day = int(date_part[6:8])
        hour= int(date_part[9:11])
        minute= int(date_part[11:13])
        second=int(date_part[13:15])

        # Define start and end dates for RCM query
        start = datetime(year, month, day, hour, minute, second)
        end_plus = start + timedelta(days=1)
        end_minus = start - timedelta(days=1)

        # -------------------------
        # QUERY +1 DAY
        # -------------------------
        rcm_plus = eodms.query(
            "RCMImageProducts",
            start, 
            end_plus, 
            features, 
            filters, 
            n_processes=4)

        print(f'Total number of products: {len(rcm_plus)}')
        rcm_plus = [result for result in rcm_plus if float(result['geodeticTerrainHeight']) <= 80.]
        print(f'Number of products at sea level: {len(rcm_plus)}')
        matches_plus = []
        rcm_plus = pd.DataFrame(rcm_plus)
        rcm_footprints_plus = [reproject_geometry(shape(rcm_plus.at[i, 'geometry']), fromEPSG=4326, toEPSG=3411) for i in range(len(rcm_plus))]

        for i in tqdm(range(len(rcm_plus))):
            if nc_footprint.intersects(rcm_footprints_plus[i]):
                matches_plus.append(rcm_plus.iloc[i]["archiveId"])

        # -------------------------
        # QUERY -1 DAY
        # -------------------------
        rcm_minus = eodms.query(
            "RCMImageProducts",
            end_minus, 
            start + timedelta(minutes=2), 
            features, 
            filters, 
            n_processes=4)  
        print(f'Total number of products: {len(rcm_minus)}')
        rcm_minus = [result for result in rcm_minus if float(result['geodeticTerrainHeight']) <= 80.]
        print(f'Number of products at sea level: {len(rcm_minus)}')
        matches_minus = []
        rcm_minus = pd.DataFrame(rcm_minus)
        rcm_footprints_minus = [reproject_geometry(shape(rcm_minus.at[i, 'geometry']), fromEPSG=4326, toEPSG=3411) for i in range(len(rcm_minus))]


        for i in tqdm(range(len(rcm_minus))):
            if nc_footprint.intersects(rcm_footprints_minus[i]):
                matches_minus.append(rcm_minus.iloc[i]["archiveId"])
              
        # -------------------------
        # DOWNLOAD MATCHES (on-the-fly)
        # -------------------------
        all_matches = set()

        all_matches.update(matches_plus)
        all_matches.update(matches_minus)

        if len(all_matches) > 0: #Only download if there are matches
            # Create a folder named after the source nc_file
            # Extract timestamp from filename e.g. 20190811T123356
            date_str = re.search(r'\d{8}T\d{6}', filename).group(0)
            nc_folder = os.path.join('/dmidata/projects/asip-cms/cgf', date_str)
            os.makedirs(nc_folder, exist_ok=True)

            uuids = list(all_matches)
            args = [(uuid, nc_folder) for uuid in uuids]
            with multiprocessing.Pool(processes=rcm_download_processes) as p:
                p.starmap(_download_helper, args)

            # -------------------------
            # CHECK SENTINEL FILE EXISTS
            # -------------------------
            sentinel_exists = any(
                fname.endswith(".SAFE.zip") for fname in os.listdir(nc_folder)
            ) if os.path.exists(nc_folder) else False

            print(f"Sentinel exists in folder: {sentinel_exists}")

            if not sentinel_exists:
                print("No Sentinel-1 file found → querying CDS...")
                base_name = filename.split("_icechart")[0]
                s1 = cds.query(
                    startDate=start,
                    endDate=start + timedelta(minutes=5),
                    collection="SENTINEL-1",
                    operationalMode="EW",
                    polarisationChannels="HH&HV",
                    nameContains=base_name,
                    aoi=north_of_50.wkt
                )     
                s1 = s1[~s1['Name'].str.contains('_COG|_CARD')]
                s1['temp_root'] = s1['Name'].apply(lambda x: "_".join(x.split('_')[:-1]))
                s1 = s1.drop_duplicates(subset=['temp_root']).drop(columns=['temp_root'])
                s1 = s1.reset_index(drop=True)

                if len(s1) > 0:
                    cds.directory = nc_folder
                    cds.download(s1)                           

        # -------------------------
        # SAVE RESULTS
        # -------------------------
        row = pd.DataFrame([{
            "nc_filename": filename,
            "SAR_images_day_after": ", ".join(matches_plus),
            "nb_SAR_images_day_after": len(matches_plus),
            "SAR_images_day_before": ", ".join(matches_minus),
            "nb_SAR_images_day_before": len(matches_minus)
        }])

        row.to_csv(
            output_path,
            mode='a',
            header=not file_exists,
            index=False
        )

        file_exists = True

        print(
            f"[{idx+1}/{total_files}] {filename} | "
            f"+1d matches: {len(matches_plus)} | "
            f"-1d matches: {len(matches_minus)}"
        )

