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


os.environ['HDF5_USE_FILE_LOCKING']='FALSE'

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

# Load .env for CDS credentials
load_dotenv()
cds = CDS(os.getenv('CDS_USR'), os.getenv('CDS_PW'))
output_path = "/dmidata/users/cgf/files/matching_results_cds_final.txt"
log_path = "/dmidata/users/cgf/files/invalid_polygons_final.txt"
total_files = sum(1 for _ in open("/dmidata/users/cgf/files/files_with_fast_ice_dmi_cis_met.txt"))

if os.path.exists(output_path):
    done_files = pd.read_csv(output_path)["nc_filename"].tolist()
    file_exists = True
else:
    done_files = []
    file_exists = False

with open("/dmidata/users/cgf/files/files_with_fast_ice_dmi_cis_met.txt", "r") as f, \
     open(log_path, "a") as log_file:  # <- open log file in append mode safely
    
    for idx, line in enumerate(f):

        nc_file = line.strip() 
        filename = os.path.basename(nc_file)

        if filename in done_files:
            print(f"Skipping {filename} (already processed)")
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

        # Define start and end dates for CDS query
        start = datetime(year, month, day, hour, minute, second)
        end_plus = start + timedelta(days=1)
        end_minus = start - timedelta(days=1)

        # -------------------------
        # QUERY +1 DAY
        # -------------------------
        s1_plus = cds.query(
            startDate=start,
            endDate=end_plus,
            collection="SENTINEL-1",
            operationalMode="EW",
            polarisationChannels="HH&HV",
            nameContains="GRDM",
            aoi=north_of_50.wkt
        )

        matches_plus = []
        if len(s1_plus) != 0:
            s1_plus = s1_plus[~s1_plus['Name'].str.contains('_COG|_CARD')]
            s1_plus['temp_root'] = s1_plus['Name'].apply(lambda x: "_".join(x.split('_')[:-1]))
            s1_plus = s1_plus.drop_duplicates(subset=['temp_root']).drop(columns=['temp_root'])
            s1_plus = s1_plus.reset_index(drop=True)

            footprints_plus = [
                reproject_geometry(shape(s1_plus.at[i, 'GeoFootprint']), 4326, 3411)
                for i in range(len(s1_plus))
            ]

            for i in tqdm(range(len(s1_plus))):
                if nc_footprint.intersects(footprints_plus[i]):
                    matches_plus.append(s1_plus.iloc[i]["Name"])
        # -------------------------
        # QUERY -1 DAY
        # -------------------------
        s1_minus = cds.query(
            startDate=end_minus,
            endDate=start + timedelta(minutes=2),
            collection="SENTINEL-1",
            operationalMode="EW",
            polarisationChannels="HH&HV",
            nameContains="GRDM",
            aoi=north_of_50.wkt
        )

        matches_minus = []
        if len(s1_minus) != 0:
            s1_minus = s1_minus[~s1_minus['Name'].str.contains('_COG|_CARD')]
            s1_minus['temp_root'] = s1_minus['Name'].apply(lambda x: "_".join(x.split('_')[:-1]))
            s1_minus = s1_minus.drop_duplicates(subset=['temp_root']).drop(columns=['temp_root'])
            s1_minus = s1_minus.reset_index(drop=True)

            footprints_minus = [
                reproject_geometry(shape(s1_minus.at[i, 'GeoFootprint']), 4326, 3411)
                for i in range(len(s1_minus))
            ]

            for i in tqdm(range(len(s1_minus))):
                if nc_footprint.intersects(footprints_minus[i]):
                    matches_minus.append(s1_minus.iloc[i]["Name"])

        # -------------------------
        # DOWNLOAD MATCHES (on-the-fly)
        # -------------------------
        all_matches = set()

        all_matches.update(matches_plus)
        all_matches.update(matches_minus)

        if len(all_matches) > 1: #Only download if there are matches (excluding the case where the only match is the nc_file itself)
            # Create a folder named after the source nc_file
            # Extract timestamp from filename e.g. 20190811T123356
            date_str = re.search(r'\d{8}T\d{6}', filename).group(0)
            nc_folder = os.path.join('/dmidata/projects/asip-cms/cgf', date_str)
            os.makedirs(nc_folder, exist_ok=True)

            # Skip already-downloaded files
            all_matches = {m for m in all_matches if not os.path.exists(os.path.join(nc_folder, f"{m}.zip"))}

            if all_matches:
                dfs = [df for df in [s1_plus, s1_minus] if isinstance(df, pd.DataFrame) and len(df) > 0]
                all_s1 = pd.concat(dfs).drop_duplicates(subset=['Name'])
                matched_rows = all_s1[all_s1["Name"].isin(all_matches)]
                cds.multi_download(matched_rows, n_processes=4, directory=nc_folder)

        # -------------------------
        # SAVE RESULTS
        # -------------------------
        row = pd.DataFrame([{
            "nc_filename": filename,
            "SAR_images_day_after": ", ".join(matches_plus),
            "nb_SAR_images_day_after": len(matches_plus) - 1,
            "SAR_images_day_before": ", ".join(matches_minus),
            "nb_SAR_images_day_before": len(matches_minus) - 1
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
            f"+1d matches: {len(matches_plus)-1} | "
            f"-1d matches: {len(matches_minus)-1}"
        )

