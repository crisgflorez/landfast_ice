# OData Downloader for Copernicus Data Space

# Utilities
import os
import pandas as pd
import requests
import getpass
import multiprocessing
from urllib.parse import quote # Added for URL encoding


process_session = None


class CDS:
    def __init__(self, username=None, password=None) -> None:
        self.usr = username
        self.pw = password
        self.collections = [
            # Copernicus Sentinel Missions
            "SENTINEL-1",
            "SENTINEL-2",
            "SENTINEL-3",
            "SENTINEL-5P",
            "SENTINEL-6",
            "SENTINEL-1-RTC",    # Sentinel-1 Radiometric Terrain Corrected

            # Complenentary data
            "GLOBAL-MOSAICS",    # Sentinel-1 and Sentinel-2 Global Mosaics
            "SMOS",              # Soil Moisture and Ocean Salinity
            "ENVISAT",           # ENVISAT- Medium Resolution Imaging Spectrometer - MERIS
            "LANDSAT-5",
            "LANDSAT-7",
            "LANDSAT-8",
            "COP-DEM",           # Copernicus DEM
            "TERRAAQUA",         # Terra MODIS and Aqua MODIS
            "S2GLC",             # S2GLC 2017
        ]
        self.directory = '.' # Default download directory
        self.keycloak_token = self._refresh_token()

    def _refresh_token(self):
        return self._get_keycloak(
            username=getpass.getpass('Username: ') if self.usr is None else self.usr,
            password=getpass.getpass('password: ') if self.pw is None else self.pw)

    def _get_keycloak(self, username: str, password: str) -> str:
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
            }
        try:
            r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
            )
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            response_content = r.content.decode() if r.content else "No content"
            raise Exception(
                f"Keycloak token creation failed. Status code: {r.status_code}. Response: {response_content}. Error: {e}"
                ) from e
        return r.json()["access_token"]

    def query(self, startDate, endDate, collection, platformSerialIdentifier=None, operationalMode=None, polarisationChannels=None, nameContains=None, cloud_cover=None, aoi=None, lon=None, lat=None, epsg=4326):
        """
        Queries the Copernicus Data Space OData API for products.

        Args:
            startDate (datetime): Start date for the query.
            endDate (datetime): End date for the query.
            collection (str): The name of the collection to query (e.g., 'SENTINEL-2').
            platformSerialIdentifier (str, optional): Filter by platform (e.g., 'S2A', 'S2B'). Defaults to None.
            operationalMode (str, optional): Filter by operational mode (e.g., 'IW'). Defaults to None.
            polarisationChannels (str, optional): Filter by polarization (e.g., 'VV VH'). Defaults to None.
            nameContains (str, optional): Filter by substring in product name. Defaults to None.
            cloud_cover (float, optional): Filter by maximum cloud cover percentage (0-100). Defaults to None.
            aoi (str, optional): Area of Interest as a WKT string (e.g., 'POLYGON((...))'). Mutually exclusive with lon/lat. Defaults to None.
            lon (float, optional): Longitude for point intersection query. Mutually exclusive with aoi. Defaults to None.
            lat (float, optional): Latitude for point intersection query. Mutually exclusive with aoi. Defaults to None.
            epsg (int, optional): EPSG code for the spatial query (aoi or lon/lat). Defaults to 4326 (WGS 84).

        Returns:
            pandas.DataFrame: A DataFrame containing the query results.

        Raises:
            ValueError: If both 'aoi' and ('lon', 'lat') are provided.
            Exception: If the API request fails.

        Note:
            MULTIPOLYGON WKT might not be supported by the API.
            Coordinates for WKT (aoi or lon/lat) must be in the specified EPSG (default 4326).
            Maximum results per query is 1000.
        """

        assert collection in self.collections, f"Collection '{collection}' not recognized." #

        if aoi is not None and (lon is not None or lat is not None):
            raise ValueError("Provide either 'aoi' or ('lon', 'lat'), not both.")

        # Base request URL
        request = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{collection}'" #

        # Add filters based on provided arguments
        if cloud_cover is not None:
            request += f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {cloud_cover:.2f})" #
        if platformSerialIdentifier is not None:
            request += f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'platformSerialIdentifier' and att/OData.CSC.StringAttribute/Value eq '{platformSerialIdentifier}')" #
        if polarisationChannels is not None:
            encoded_polarisation = quote(polarisationChannels) #
            request += f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'polarisationChannels' and att/OData.CSC.StringAttribute/Value eq '{encoded_polarisation}')" #
        if operationalMode is not None:
            request += f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq '{operationalMode}')" #
        if nameContains is not None:
            request += f" and contains(Name, '{nameContains}')" # Adjusted from 'name' to 'Name' based on typical OData conventions, verify if needed

        # --- Spatial Filter ---
        if aoi is not None:
             # Ensure AOI string doesn't contain problematic characters or encode if necessary
            encoded_aoi = quote(aoi) # Simple encoding, might need refinement based on WKT complexity
            request += f" and OData.CSC.Intersects(area=geography'SRID={epsg};{encoded_aoi}')" #
        elif lon is not None and lat is not None:
            # Construct POINT WKT for point intersection
            point_wkt = f"POINT({lon} {lat})"
            encoded_point_wkt = quote(point_wkt) # Encode the point WKT
            request += f" and OData.CSC.Intersects(area=geography'SRID={epsg};{encoded_point_wkt}')"
        # --- End Spatial Filter ---

        if startDate is not None and endDate is not None:
            request += f" and ContentDate/Start ge {startDate.strftime('%Y-%m-%dT%H:%M:%SZ')} and ContentDate/End le {endDate.strftime('%Y-%m-%dT%H:%M:%SZ')}" # Adjusted filter logic and removed milliseconds

        request += "&$expand=Attributes" #
        request += "&$top=1000" # max results is 1000

        #print(f"Executing OData Query: {request}") # Optional: print the query URL for debugging

        headers = {"Authorization": f"Bearer {self.keycloak_token}"}
        try:
            response = requests.get(request, headers=headers)
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            json_data = response.json()
        except requests.exceptions.RequestException as e:
            response_content = response.content.decode() if response and response.content else "No content"
            raise Exception(f"OData query failed. Status: {response.status_code if response else 'N/A'}. Response: {response_content}. Error: {e}") from e
        except requests.exceptions.JSONDecodeError:
             raise Exception(f"Failed to decode JSON response from OData query. Response: {response.text}")


        if 'value' not in json_data:
             print(f"Warning: 'value' key not found in the response JSON: {json_data}")
             return pd.DataFrame() # Return empty DataFrame if no results or unexpected format

        try:
            return pd.DataFrame.from_dict(json_data['value']) #
        except Exception as e:
            print(f"Error converting JSON response to DataFrame: {e}") #
            print(f"JSON Response was: {json_data}") #
            return pd.DataFrame() # Return empty DataFrame on conversion error


    # --- Download methods remain the same ---
    def download(self, df):
        # Check if token needs refreshing (optional, depends on token lifetime)
        # self.keycloak_token = self._refresh_token() # Uncomment if needed

        session = requests.Session() #
        session.headers.update({'Authorization': f'Bearer {self.keycloak_token}'}) #

        os.makedirs(self.directory, exist_ok=True) # Ensure download directory exists

        download_count = 0
        for i in range(len(df)):
            product_id = df.iloc[i]['Id'] #
            product_name = df.iloc[i]['Name'] #
            download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value" #
            file_path = os.path.join(self.directory, f"{product_name}.zip") #

            print(f"Attempting to download: {product_name} ({product_id})")

            try:
                # Initial request, allow redirects should be False initially to handle potential redirects manually
                response = session.get(download_url, allow_redirects=False, stream=True) #
                response.raise_for_status() # Check for initial request errors

                # Handle potential redirects (e.g., to S3)
                while response.status_code in (301, 302, 303, 307) and 'Location' in response.headers: #
                    redirect_url = response.headers['Location'] #
                    print(f"Redirecting to: {redirect_url}")
                    # For S3 URLs etc., we might not need the auth header, or a different one.
                    # For simplicity here, we'll use the same session which carries the header.
                    response = session.get(redirect_url, allow_redirects=False, stream=True) #
                    response.raise_for_status()


                # After handling redirects, proceed with download if status is OK (200)
                if response.status_code == 200:
                     with open(file_path, 'wb') as p: #
                         for chunk in response.iter_content(chunk_size=8192):
                             if chunk: # filter out keep-alive new chunks
                                 p.write(chunk) #
                     print(f"Successfully downloaded: {file_path}")
                     download_count += 1
                else:
                    print(f"Failed to download {product_name}. Final Status code: {response.status_code}")
                    print(f"Response content: {response.text}")


            except requests.exceptions.RequestException as e:
                 print(f"Error downloading {product_name}: {e}")
                 # Optionally, print response content if available
                 if response:
                     print(f"Response status: {response.status_code}, content: {response.text}")


        print(f"Finished download process. Downloaded {download_count} out of {len(df)} products.")


    def _download(self, product_info):
        """Helper function for multi_download."""
        global process_session # Use the process-global session variable

        product_id = product_info.get('Id') or product_info.get('id')
        product_name = product_info.get('Name') or product_info.get('name')

        if not product_id or not product_name:
             print(f"Skipping download due to missing ID or Name in product info: {product_info}")
             return

        # --- MODIFIED SECTION: Initialize session once per process ---
        if process_session is None:
            print(f"Process {os.getpid()}: Initializing session...")
            try:
                # Get one token for this process
                current_token = self._refresh_token() 
                process_session = requests.Session()
                process_session.headers.update({'Authorization': f'Bearer {current_token}'})
            except Exception as e:
                print(f"Process {os.getpid()}: Failed to initialize token/session for {product_name}: {e}")
                return # Can't download this or subsequent items
        # --- END MODIFIED SECTION ---

        # Use the persistent session for this process
        session = process_session 

        download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        file_path = os.path.join(self.directory, f"{product_name}.zip.tmp")

        print(f"Process {os.getpid()}: Attempting to download {product_name}")

        response = None # Define response here to access it in except block
        try:
            response = session.get(download_url, allow_redirects=False, stream=True)
            response.raise_for_status()

            while response.status_code in (301, 302, 303, 307) and 'Location' in response.headers:
                redirect_url = response.headers['Location']
                print(f"Process {os.getpid()}: Redirecting to: {redirect_url}")
                response = session.get(redirect_url, allow_redirects=False, stream=True)
                response.raise_for_status()

            if response.status_code == 200:
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1048576):
                        if chunk:
                            file.write(chunk)
                print(f"Process {os.getpid()}: Successfully downloaded {file_path.replace('.tmp', '')}")
                os.rename(file_path, file_path.replace('.tmp', ''))
            else:
                print(f"Process {os.getpid()}: Failed to download {product_name}. Final Status code: {response.status_code}")
                print(f"Process {os.getpid()}: Response: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Process {os.getpid()}: Error downloading {product_name}: {e}")
            if response is not None:
                print(f"Process {os.getpid()}: Response status: {response.status_code}, content: {response.text}")
                
                # --- IMPROVEMENT: Handle token expiry ---
                # If we got an auth error, reset the session.
                # The next download in this process will force a re-authentication.
                if response.status_code in (401, 403):
                    print(f"Process {os.getpid()}: Authorization error. Token may have expired. Resetting session.")
                    process_session = None 
            else:
                print(f"Process {os.getpid()}: Request failed without a response (e.g., connection error).")


    def multi_download(self, products, n_processes=4, directory='.'):
        """Downloads multiple products in parallel."""
        if isinstance(products, pd.DataFrame):
             # Convert DataFrame rows to list of dictionaries for multiprocessing
             products_list = products.to_dict('records')
        elif isinstance(products, list) and all(isinstance(p, dict) for p in products):
             products_list = products
        else:
             raise ValueError("Input 'products' must be a pandas DataFrame or a list of dictionaries.")

        self.directory = directory #
        os.makedirs(self.directory, exist_ok=True)

        print(f"Starting parallel download of {len(products_list)} products using {n_processes} processes...")

        # Use multiprocessing Pool
        with multiprocessing.Pool(n_processes) as pool: #
            pool.map(self._download, products_list) #

        print("Finished parallel download.")