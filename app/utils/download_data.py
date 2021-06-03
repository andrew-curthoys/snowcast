import requests
import config as cfg

class Downloader:
    def __init__(self):
        self.year = cfg.year
        self.buoy_data_base = cfg.buoy_data_loc['base_site']

    def get_current_year(self, buoy_id: str, report_type: str):
        for i, mth in enumerate(cfg.buoy_data_months()):
            mth_int = i + 1
            dl_path = self.buoy_data_base + f'/data/{report_type}/{mth}/{buoy_id}{mth_int}{self.year}.txt.gz'
            response = requests.get(dl_path)
            if response.status_code == 404:
                break