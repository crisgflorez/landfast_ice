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

os.environ['HDF5_USE_FILE_LOCKING']='FALSE'

def get_processor(sar_file):
    fname = os.path.basename(sar_file)

    if fname.startswith("RCM"):
        return RCMProcessor(sar_file)
    elif fname.startswith("S1"):
        return S1Processor(sar_file)
    else:
        raise ValueError(f"Unknown SAR product type: {fname}")
    
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


base_path = "/dmidata/projects/asip-cms/cgf"
nc_base_path = "/dmidata/projects/ai4arctic/asidv3/dataset_files"
csv_path = "/dmidata/users/cgf/files/overlap_results_SAR_images_associated_nc.csv"
error_csv_path = "/dmidata/users/cgf/files/sar_files_associated_nc_with_errors.csv"
missing_csv = "/dmidata/users/cgf/files/missing_files_associated_nc.csv"

if os.path.exists(csv_path):
    df_existing = pd.read_csv(csv_path)
    already_done_folders = set(df_existing["folder"].unique())
else:
    df_existing = pd.DataFrame(columns=[
        "folder",
        "sar_file",
        "base_file",
        "overlap_sar_pct",
        "overlap_nc_pct",
        "time_diff_hours"
    ])
    already_done_folders = set()

for name in os.listdir(base_path):
    if name == "data_before_20180626":
        continue

    if name in already_done_folders:
        print(f"Skipping already processed folder: {name}")
        continue

    path = os.path.join(base_path, name)

    timestamp = os.path.basename(path)
    matches = glob.glob(os.path.join(path, f"*_{timestamp}_*_*_*_*"))
    if len(matches) == 0:
        print(f"Missing SAR_file_based_nc in folder: {name}")

        error_row = {
            "folder": name,
            "expected_pattern": f"*_{timestamp}_*_*_*_*",
            "error": "SAR_file_based_nc not found"
        }

        pd.DataFrame([error_row]).to_csv(
            missing_csv,
            mode='a',
            header=not os.path.exists(missing_csv),
            index=False
        )
        continue
    SAR_file_based_nc = matches[0]
    SAR_file_based_nc_cutted ="_".join(os.path.basename(SAR_file_based_nc).split("_")[:8])
    nc_file=glob.glob(os.path.join(nc_base_path, f"*{SAR_file_based_nc_cutted}*"))[0]

    #Open a netCDF file containing ice chart data with fast ice
    ds = xr.open_dataset(nc_file) 
    #In the netcdf file the coordinates of the CGPs are saved in 
    #ds.sar_grid_latitude and ds.sar_grid_longitude
    #These values have been already corrected to be at sea level
    #lats = ds.sar_grid_latitude.values
    #lons = ds.sar_grid_longitude.values
    #points = [Point(lon, lat) for lon, lat in zip(lons, lats)]
    #footprint = MultiPoint(points).convex_hull
    #nc_dts = get_timestamp(os.path.basename(nc_file))
    #nc_footprint=reproject_geometry(footprint, fromEPSG=4326, toEPSG=3411)

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
                
        # Create a polygon from the edges (convex hull or alpha shape per polygon)
        poly = MultiPoint(np.column_stack((lons_poly, lats_poly))).convex_hull
            
        polygons.append(poly)

    nc_footprint = MultiPolygon(polygons)
    #To remove small areas duplicated in multiple polygons
    nc_footprint = unary_union(polygons)
    nc_footprint = reproject_geometry(nc_footprint, fromEPSG=4326, toEPSG=3411)

    base_timestamp = get_timestamp(SAR_file_based_nc)

    # =========================
    # 4. LOOP OVER ALL SAR FILES
    # =========================
    sar_files = glob.glob(os.path.join(path, "*.SAFE.zip"))

    df_folder = df_existing[df_existing["folder"] == name]
    already_done_sar_files = set(df_folder["sar_file"].unique())

    for sar_file in sar_files:

        # skip other files that are not base file
        if sar_file != SAR_file_based_nc:
            continue
        if os.path.basename(sar_file) in already_done_sar_files:
            continue

        sar_timestamp = get_timestamp(sar_file)
        time_diff_hours = (sar_timestamp - base_timestamp).total_seconds() / 3600
        
        try:
            s1p = get_processor(sar_file)
            # We transform to 3411 because we need a planar projection to set the GCPs to sea level and to upsample the GCP grid accurately (too much distortion in lat/lon). 
            s1p._transform_gcps(3411)
            s1p._set_gcps_to_sea_level()

            gcp_points = MultiPoint(
                list(zip(s1p.gcps['lon'], s1p.gcps['lat']))
            )

            gcp_polygon = gcp_points.convex_hull

            # =========================
            # 5. INTERSECTION
            # =========================
            intersection = gcp_polygon.intersection(nc_footprint)

            gcp_area = gcp_polygon.area
            nc_area = nc_footprint.area
            intersection_area = intersection.area

            #how much fast ice is inside ground control points of this new SAR image
            overlap_pct = (intersection_area / gcp_area) * 100 if gcp_area > 0 else np.nan
            #how much fast ice of the ice chart is covered in this new SAR image 
            overlap_pct_nc = (intersection_area / nc_area) * 100 if nc_area > 0 else np.nan

            print(overlap_pct)
            print(overlap_pct_nc)   
            
            # =========================
            # 6. SAVE RESULT
            # =========================
            result_row = {
                "folder": name,
                "sar_file": os.path.basename(sar_file),
                "base_file": os.path.basename(SAR_file_based_nc),
                "overlap_sar_pct": overlap_pct,
                "overlap_nc_pct": overlap_pct_nc,
                "time_diff_hours": time_diff_hours
            }

            df_row = pd.DataFrame([result_row])

            # write header only if file doesn't exist
            df_row.to_csv(
                csv_path,
                mode='a',
                header=not os.path.exists(csv_path),
                index=False
            )

        except Exception as e:
            print(f"Error with {sar_file}: {e}")

            error_row = {
                "folder": name,
                "sar_file": os.path.basename(sar_file),
                "base_file": os.path.basename(SAR_file_based_nc),
                "error": str(e)
            }

            pd.DataFrame([error_row]).to_csv(
                error_csv_path,
                mode='a',
                header=not os.path.exists(error_csv_path),
                index=False
            )

            continue
