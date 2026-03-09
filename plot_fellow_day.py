import numpy as np
from eoutils import S1Processor
from tqdm import tqdm
import xarray as xr
import cartopy
import matplotlib.pyplot as plt
import geopandas as gpd
from datetime import datetime
import pandas as pd
import geopandas as gpd

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
def reproject_geometry(geom, fromEPSG=4326, toEPSG=3411):
    import shapely
    from pyproj import Transformer

    transformer = Transformer.from_crs(fromEPSG, toEPSG, always_xy=True)

    return shapely.transform(geom, transformer.transform, interleaved=False)

def transform_points(x, y, fromEPSG, toEPSG):
    import pyproj
    from pyproj import Transformer

    transformer = Transformer.from_crs(pyproj.CRS(f'EPSG:{fromEPSG}'), pyproj.CRS(f'EPSG:{toEPSG}'), always_xy=True)
    x, y = transformer.transform(x, y)

    return x, y

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

def get_amsr3_dt(amsr3_path):
    import os
    import re
    import dateutil

    pattern = r'GGWAM3_(\d{12})'
    match = re.search(pattern, amsr3_path)

    if match:
        timestamp = match.group(1)
    else:
        print(f"No timestamp found for file {os.path.basename(amsr3_path)}.")

    return dateutil.parser.parse(timestamp)

def get_ASIP_L2_timestamp(file):
    import os
    import re
    import dateutil

    pattern = re.compile(r"(\d{4}\d{2}\d{2}[_T]\d{2}\d{2}\d{2})") # e.g. 20251023T102620 (S1), 20251008_225228 (RCM)
    # For Sentinel-1, this script graps the first timestamp (the start time)

    match = re.search(pattern, os.path.basename(file))
    if match:
        matched_str = match.group(1)
        timestamp_str = matched_str.replace("_", "").replace("T", "")
        return dateutil.parser.parse(timestamp_str)
    else:
        raise TypeError(f"The following files has no matching timestamp: {file}")
    



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
    


zips = [
    '/dmidata/projects/asip-cms/sentinel1/2021/02/08/S1A_EW_GRDM_1SDH_20210208T082107_20210208T082207_036496_044911_CA5C.zip',
    '/dmidata/projects/asip-cms/sentinel1/2021/02/10/S1B_EW_GRDM_1SDH_20210210T183506_20210210T183610_025548_030B57_6234.zip'
]



RGBs, Xs, Ys = [], [], []

dualpol2rgb = DualPol2RGB()
for n in tqdm(range(len(zips))):
    s1p = S1Processor(_zip=zips[n])
    s1p._transform_gcps(3411)
    s1p._set_gcps_to_sea_level()
    gcp_grid_shape = (len(s1p.gcps['sample'][s1p.gcps['sample'] == 0]), len(s1p.gcps.line[s1p.gcps.line == 0]))
    X, Y = upsample_gcp_grid_RectBiSpl(s1p.gcps.line.values.reshape(gcp_grid_shape),
        s1p.gcps['sample'].values.reshape(gcp_grid_shape),
        s1p.gcps.lon.values.reshape(gcp_grid_shape),
        s1p.gcps.lat.values.reshape(gcp_grid_shape),
        s1p.shape)
    HH, HV = s1p.process(calib='sigma', remove_thermal_noise=True)
    rgb = dualpol2rgb.create_rgb(HH, HV, HV/HH)

    skip = 6
    RGBs.append(rgb[::skip, ::skip, :])
    Xs.append(X[::skip, ::skip])
    Ys.append(Y[::skip, ::skip])



nc = '/dmidata/projects/ai4arctic/asidv3/dataset_files/S1A_EW_GRDM_1SDH_20210208T082107_20210208T082207_036496_044911_CA5C_icechart_dmi_202102080820_CentralEast_RIC.nc'
#nc = '/dmidata/projects/ai4arctic/asidv3/dataset_files/S1B_EW_GRDM_1SDH_20210210T183506_20210210T183610_025548_030B57_6234_icechart_dmi_202102101835_CentralEast_RIC.nc'

ds = xr.open_dataset(nc)
# Convert xarray DataArray to a list of strings
lines = ds.polygon_codes.values.tolist()

# The first line contains column names
columns = lines[0].split(';')

# The rest are data rows
data = [line.split(';') for line in lines[1:]]

# Make a DataFrame
df = pd.DataFrame(data, columns=columns)

# Convert relevant columns to numeric (they are strings)
df['poly_id'] = df['poly_id'].astype(int)
df['CT'] = df['CT'].astype(int)
df['FA'] = df['FA'].astype(int)

# Filter for CT == 92 and FA == 8
result = df[(df['CT'] == 92) & (df['FA'] == 8)]

# Get poly_id values
poly_ids = result['poly_id'].tolist()
print(poly_ids)
# Extract the first timestamp (after S1A_EW_GRDM_1SDH_)
basename = os.path.basename(nc)
timestamp_str = basename.split('_')[4]  # '20210208T082107'

# Convert to datetime
dt = datetime.strptime(timestamp_str, '%Y%m%dT%H%M%S')

# Format as desired
title_str = dt.strftime('%d/%m/%Y %H:%M:%Sh')

s1p = S1Processor(_zip=zips[0])
s1p._transform_gcps(3411)
s1p._set_gcps_to_sea_level()
gcp_grid_shape = (len(s1p.gcps['sample'][s1p.gcps['sample'] == 0]), len(s1p.gcps.line[s1p.gcps.line == 0]))
X, Y = upsample_gcp_grid_RectBiSpl(s1p.gcps.line.values.reshape(gcp_grid_shape),
    s1p.gcps['sample'].values.reshape(gcp_grid_shape),
    s1p.gcps.lon.values.reshape(gcp_grid_shape),
    s1p.gcps.lat.values.reshape(gcp_grid_shape),
    s1p.shape)

ice = ds['polygon_icechart'].values
# poly_ids is your list of poly_ids you want to mask
mask = np.isin(ice, poly_ids).astype('float32')  # 1 where ice matches any poly_id
mask[mask == 0] = np.nan   
mask_temp = np.nan_to_num(mask, nan=0)
skip = 1  # igual que antes
fig, ax = plt.subplots(1, 1, figsize=(16, 16), subplot_kw={'projection': NorthPolStere()})
ax.set_facecolor('black')

ax.add_geometries(
    gdf.geometry,
    crs=NorthPolStere(),
    facecolor='beige',
    alpha=0.3,            # transparente
    edgecolor='black',
    linewidth=0.35,
    zorder=90
)

# Dibujar RGB SAR como fondo
ax.pcolormesh(
    Xs[0][::skip, ::skip],
    Ys[0][::skip, ::skip],
    RGBs[0][::skip, ::skip],
    transform=NorthPolStere(),
    zorder=100
)
extent = ax.get_extent()

# Dibujar solo los contornos de los polígonos
ax.contour(
    X[::skip, ::skip],
    Y[::skip, ::skip],
    mask_temp[::skip, ::skip],
    levels=[0.5],       # nivel que separa 0 y 1
    colors='red',     # contorno blanco
    linewidths=5,
    transform=NorthPolStere(),
    zorder=120
)
fig.suptitle(title_str, fontsize=60, color='black', y=0.97)
plt.subplots_adjust(top=0.96)  # move the plotting area up, giving more room for suptitle
plt.show()




# ----- Preparar datos -----
# Leer ice chart
ds = xr.open_dataset(nc)
ice = ds['polygon_icechart'].values.astype('float32')
ice[ice < 0] = np.nan  # quitar valores inválidos

# Obtener poly_ids únicos reales
poly_ids = np.unique(ice[~np.isnan(ice)]).astype(int)
n_polys = len(poly_ids)

# Crear colormap discreto con tantos colores como poly_ids
cmap = plt.cm.get_cmap('tab20', n_polys)

# Crear un mapeo de poly_id -> índice 0..N-1
poly_to_index = {pid: i for i, pid in enumerate(poly_ids)}

# Crear nueva matriz con índices consecutivos
indexed = np.full_like(ice, np.nan)

for pid in poly_ids:
    indexed[ice == pid] = poly_to_index[pid]

# ---- Plot ----
skip = 1
fig, ax = plt.subplots(1, 1, figsize=(16, 16),
                       subplot_kw={'projection': NorthPolStere()})
ax.set_facecolor('black')

mesh = ax.pcolormesh(
    X[::skip, ::skip],
    Y[::skip, ::skip],
    indexed[::skip, ::skip],
    transform=NorthPolStere(),
    cmap=cmap,
    zorder=100
)

# Colorbar con etiquetas reales
cbar = fig.colorbar(mesh, ax=ax, orientation='vertical')
cbar.set_ticks(range(n_polys))
cbar.set_ticklabels(poly_ids)
cbar.set_label('poly_id')

plt.show()



#plt.savefig('cristina_plot_1.png', dpi=250, bbox_inches='tight')