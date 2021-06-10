import requests
import chardet
import config as cfg
import gzip
import os
import sqlite3

from io import BytesIO

class Downloader:
    def __init__(self):
        self.year = cfg.year
        self.buoy_data_base = cfg.buoy_data_loc['base_site']

    def buoy_current_year(self, buoy_id: str, report_type: str):
        """Method to download all data for the current year
        
        Arguments:
            buoy_id: Identification number of the buoy
            report_type: Type of report that the buoy records, the types are listed in the README
        Returns:
            data: data from the buoy report
        """
        for i, mth in enumerate(cfg.buoy_data_months()):
            mth_int = i + 1
            dl_path = self.buoy_data_base + f'data/{report_type}/{mth}/{buoy_id}{mth_int}{self.year}.txt.gz'
            response = requests.get(dl_path)
            if response.status_code == 404:
                break
            content = gzip.decompress(response.content)
            data_str = content.decode('ascii')
            self.build_query(data_str, 'buoy_insert')
    

    def build_query(self, data_str: str, query_type: str):
        data = data_str.split('/n')
        pass

    def write_to_db(self, query):
        db_file = os.path.abspath('../../data/snowcast.db')
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(query)


if __name__ == '__main__':
    dler = Downloader()
    dler.buoy_current_year('51001', 'stdmet')
    dler.write_to_db()
