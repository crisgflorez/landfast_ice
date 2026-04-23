import pandas as pd

output_path = "/dmidata/users/cgf/files/matching_results_cds_final.txt"
df = pd.read_csv(output_path, sep=",")



#Download Sentinel-1 matches in /dmidata/projects/asip-cms/cgf
path_directoy_matches='/dmidata/projects/asip-cms/cgf/s1_data'
cds.multi_download(s1_results.iloc[list(s1_matches)], n_processes=4, directory=path_directoy_matches)

#Plot example
file1=path_directoy_matches+'/S1A_EW_GRDM_1SDH_20190811T132352_20190811T132452_028522_03398D_A190.SAFE.zip'

file2=path_directoy_matches+'/S1B_EW_GRDM_1SDH_20190811T123356_20190811T123456_017538_020FC3_8562.SAFE.zip'

zips=[file1, file2]

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

plt.imshow(HH,cmap='gray',vmin=np.nanpercentile(HH,2),vmax=np.nanpercentile(HH,98))


fig, ax = plt.subplots(1, 1, figsize=(16, 16), subplot_kw={'projection': NorthPolStere()})
ax.set_facecolor('black')


# plot SAR
ax.pcolormesh(
    Xs[0][::skip, ::skip],
    Ys[0][::skip, ::skip],
    RGBs[0][::skip, ::skip],
    transform=NorthPolStere(),
    zorder=100
)
extent = ax.get_extent()

#fig.suptitle(title_str, fontsize=60, color='black', y=0.97)
plt.subplots_adjust(top=0.96)  # move the plotting area up, giving more room for suptitle
plt.show()








count_non_zero = 0

with open("/dmidata/users/cgf/files/s1_results_counts.txt", "r") as f:
    for line in f:
        # Split "filename: value"
        parts = line.strip().split(":")
        value = int(parts[1].strip())

        if value != 0:
            count_non_zero += 1

print("Files with non-zero results:", count_non_zero)

#There is one column containing the GeoFootprint
s1_results['GeoFootprint']

# We open one SAR file already downloaded
file='/dmidata/projects/asip-cms/sentinel1/2018/01/29/S1B_EW_GRDM_1SDH_20180129T110712_20180129T110812_009385_010DB9_443B.zip'
s1p=S1Processor(file)
#This transform the GCPs from geographic coordinates north stereographic projection
s1p._transform_gcps(3411)

#Estimation of the footprint of SAR image based on the GCPs, by creating a convex hull around the GCPs. The convex hull is the smallest convex polygon that can enclose all the GCPs, and it can be used as an approximation of the footprint of the SAR image.
s1p._get_gcps_as_geopandas_df().union_all().convex_hull


with open('/dmidata/users/cgf/files/eastern_arctic_polygon.json', 'r') as f:
    eastern_arctic_geojson = json.load(f)

with open('/dmidata/users/cgf/files/western_arctic_polygon.json', 'r') as f:
    western_arctic_geojson = json.load(f)

eodms = EODMS(os.getenv('EODMS_USR'), os.getenv('EODMS_PW'))

features = [
    ('intersects', eastern_arctic_geojson),
    ('intersects', western_arctic_geojson),
]

filters = {'Beam Mnemonic': ('LIKE', ['%SCLN%']),#, '%SC50M%', '%SC100M%']),
            'Polarization': ('=', 'HH HV')}

rcm_results = eodms.query(
    "RCMImageProducts",
    start, 
    end, 
    features, 
    filters, 
    n_processes=4)

print(f'Total number of products: {len(rcm_results)}')
rcm_results = [result for result in rcm_results if float(result['geodeticTerrainHeight']) <= 80.]
print(f'Number of products at sea level: {len(rcm_results)}')
rcm_results = pd.DataFrame(rcm_results)



s1_dts = s1_results['Name'].apply(get_timestamp).to_list()
rcm_dts = rcm_results['supplierOrderNumber'].apply(get_timestamp).to_list()

s1_footprints = [reproject_geometry(shape(s1_results.at[i, 'GeoFootprint']), fromEPSG=4326, toEPSG=3411) for i in range(len(s1_results))]
rcm_footprints = [reproject_geometry(shape(rcm_results.at[i, 'geometry']), fromEPSG=4326, toEPSG=3411) for i in range(len(rcm_results))]


# Match criteria:
# Within 15 minutes and minium 50% coverage of Sentinel-1 footprint

temporal_window_minutes = 15
intersection_pct = 0.75

s1_matches = []
for i in tqdm(range(len(s1_results))):
    for j in range(len(slstr_results)):
        if slstr_dts[j] > s1_dts[i] - timedelta(minutes=temporal_window_minutes) and slstr_dts[j] < s1_dts[i] + timedelta(minutes=temporal_window_minutes):
            if slstr_footprints[j].intersects(s1_footprints[i]) and s1_footprints[i].intersection(slstr_footprints[j]).area / s1_footprints[i].area > intersection_pct:
                s1_matches.append((i, j))

rcm_matches = []
for i in tqdm(range(len(rcm_results))):
    for j in range(len(slstr_results)):
        if slstr_dts[j] > rcm_dts[i] - timedelta(minutes=temporal_window_minutes) and slstr_dts[j] < rcm_dts[i] + timedelta(minutes=temporal_window_minutes):
            if slstr_footprints[j].intersects(rcm_footprints[i]) and rcm_footprints[i].intersection(slstr_footprints[j]).area / rcm_footprints[i].area > intersection_pct:
                rcm_matches.append((i, j))

print(len(s1_matches))
print(len(rcm_matches))