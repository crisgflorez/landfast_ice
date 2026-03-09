"""
Class to query, order and download products from EODMS. Requires vetted user for RCM privileges.
"""

import os
import multiprocessing
from datetime import timedelta
from eodms_rapi import EODMSRAPI
import polars as pl


class EODMS():
    def __init__(self, username, password):
        self.username = username
        self.password = password
    
    def _get_dates(self, start_datetime, end_datetime, hour_interval=2):
        """
        Split the time range into hour intervals
        """

        dates = []
        current_datetime = start_datetime
        while current_datetime < end_datetime:
            next_datetime = current_datetime + timedelta(hours=hour_interval)
            if next_datetime > end_datetime:
                next_datetime = end_datetime
            dates.append({"start": current_datetime.strftime("%Y%m%d_%H%M%S"), "end": next_datetime.strftime("%Y%m%d_%H%M%S")})
            current_datetime = next_datetime

        return dates
    
    def _get_hour_interval(self, start_datetime, end_datetime, n_processes):
        """
        Get the hour interval to split the time range into n_processes
        """

        return ((end_datetime - start_datetime).total_seconds() / 3600)/n_processes

    def _search(self):
        """
        Search the EODMS API for satellite images using a single process
        """

        rapi = EODMSRAPI(self.username, self.password)
        rapi.search(self.collection, features=self.features, dates=self.dates, filters=self.filters, max_results=self.max_results)
        results = rapi.get_results('full')
        rapi.clear_results()
        return results
    
    def _search2(self, date, results):
        """
        Used as base function for multiprocessing
        """

        rapi = EODMSRAPI(self.username, self.password)
        rapi.search("RCMImageProducts", features=self.features, dates=[date], filters=self.filters, max_results=10000, )
        results.append(rapi.get_results('full'))
        rapi.clear_results()

    def _multi_search(self):
        """
        Search the EODMS API for satellite images using multiple processes
        """

        with multiprocessing.Manager() as manager:
            results = manager.list()
            with multiprocessing.Pool(self.n_processes) as p:
                p.starmap(self._search2, [(date, results) for date in self.dates])
            return [item for sublist in results for item in sublist]
        
    def _load_order_res(self):
        """
        Load the order results from a Parquet file
        """

        order_df = pl.read_parquet('orders.pq')
        order_res = {'items': [{'recordId': row['recordId'], 'itemId': row['itemId'], 'orderId': row['orderId'], 'collectionId': row['collectionId'], 'status': row['status'], 'dateRapiOrdered': row['dateRapiOrdered']} for row in order_df.to_dicts()]}

        return order_res
    
    def _order(self, results, priority, parameters, order_res_list):
        """
        Base function for multiple orders using multiprocessing
        """

        rapi = EODMSRAPI(self.username, self.password)
        order_res = rapi.order([results] if not isinstance(results, list) else results, priority=priority, parameters=parameters)
        order_res_list.append(order_res['items'])

    def query(self, collection, start_datetime, end_datetime, features, filters, max_results=1000, n_processes=4, hour_interval=None):
        """
        Query the EODMS API for a collection of satellite images
        """

        assert n_processes is None or n_processes > 0, "n_processes must be greater than 0"
        if n_processes < 2:
            assert hour_interval is None, "hour_interval must be None if n_processes < 2"

        self.collection = collection
        self.features = features
        self.filters = filters
        self.max_results = max_results
        self.n_processes = n_processes
        self.hour_interval = self._get_hour_interval(start_datetime, end_datetime, n_processes) if hour_interval is None else hour_interval if hour_interval is None else hour_interval
        self.dates = self._get_dates(start_datetime, end_datetime, hour_interval=self.hour_interval)

        return self._multi_search() if n_processes > 1 else self._search()
    
    def order(self, results, priority='medium', parameters=[{"packagingFormat": "TAR"}], n_processes=1):
        """
        Order the satellite images in parallel
        """

        results = results if isinstance(results, list) else [results]
        assert len(results) >= n_processes, "The number of results must be greater than or equal to the number of processes"

        if len(results) > 1:
            # Split the results into chunks
            chunk_size = len(results) // n_processes
            chunks = [results[i * chunk_size:(i + 1) * chunk_size] for i in range(n_processes)]
            if len(results) % n_processes != 0:
                chunks[-1].extend(results[n_processes * chunk_size:])

            with multiprocessing.Manager() as manager:
                order_res_list = manager.list()
                with multiprocessing.Pool(n_processes) as p:
                    p.starmap(self._order, [(chunk, priority, parameters, order_res_list) for chunk in chunks])
                
                # Flatten the list of order results
                self.order_res = {'items': [item for sublist in order_res_list for item in sublist]}
                
                # Create a DataFrame to store the aggregated order results
                order_df = pl.DataFrame({
                    'recordId': [item['recordId'] for item in self.order_res['items']],
                    'itemId': [item['itemId'] for item in self.order_res['items']],
                    'orderId': [item['orderId'] for item in self.order_res['items']],
                    'collectionId': [item['collectionId'] for item in self.order_res['items']],
                    'status': [item['status'] for item in self.order_res['items']],
                    'dateRapiOrdered': [item['dateRapiOrdered'] for item in self.order_res['items']]
                    })

                # Save the DataFrame to a Parquet file
                order_df.write_parquet('orders.pq')
        else:
            rapi = EODMSRAPI(self.username, self.password)
            self.order_res = rapi.order(results, priority=priority, parameters=parameters)

            # Create a DataFrame to store the aggregated order results
            order_df = pl.DataFrame({
                'recordId': [item['recordId'] for item in self.order_res['items']],
                'itemId': [item['itemId'] for item in self.order_res['items']],
                'orderId': [item['orderId'] for item in self.order_res['items']],
                'collectionId': [item['collectionId'] for item in self.order_res['items']],
                'status': [item['status'] for item in self.order_res['items']],
                'dateRapiOrdered': [item['dateRapiOrdered'] for item in self.order_res['items']]
                })

            # Save the DataFrame to a Parquet file
            order_df.write_parquet('orders.pq')

        return self.order_res
    
    def _download(self, order_res=None, dest=os.getcwd(), max_attempts=None):
        """
        Download the satellite images
        """

        rapi = EODMSRAPI(self.username, self.password)
        order_res = self._load_order_res() if order_res is None else order_res
        rapi.download(order_res, dest=dest, max_attempts=max_attempts)

    def _multi_download(self, order_res, dest, max_attempts=None):
        """
        Base function for multiple downloads using multiprocessing
        """
        
        rapi = EODMSRAPI(self.username, self.password)
        rapi.download(order_res, dest=dest, max_attempts=max_attempts)

    def download(self, order_res=None, dest=os.getcwd(), n_processes=1, max_attempts=None):
        """
        Download the satellite images in parallel
        """

        order_res = self._load_order_res() if order_res is None else order_res
        if n_processes == 1:
            self._download(order_res, dest, max_attempts)
        else:
            grouped_orders = [{'items': order_res['items'][i:i + n_processes]} for i in range(0, len(order_res['items']), n_processes)]
            with multiprocessing.Pool(n_processes) as p:
                p.starmap(self._multi_download, [(grouped_order, dest, max_attempts) for grouped_order in grouped_orders])
        
