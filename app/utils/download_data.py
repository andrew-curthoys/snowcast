import requests
import chardet
from requests.models import Response
import config as cfg
import gzip
import os
import re
import sqlite3

from datetime import datetime
from pathlib import Path
from io import BytesIO

class Downloader:
	def __init__(self):
		self.year = cfg.year
		self.buoy_base_url = cfg.buoy_data_meta['base_url']

	def buoy_current_year_dl(self, buoy_id: str, report_type: str = 'stdmet'):
		"""Downloads all data for the current year
		
		Arguments:
			buoy_id: Identification number of the buoy
			report_type: Type of report that the buoy records, the types are listed in the README.
				Standard report type is Standard Meterological
		"""
		table = 'buoy_data'
		for i, mth in enumerate(cfg.buoy_data_months()):
			mth_int = i + 1
			dl_url = self.buoy_base_url + f'data/{report_type}/{mth}/{buoy_id}{mth_int}{self.year}.txt.gz'
			data_str = self.get_site_content(dl_url)
			if data_str == 404:
				break
			query = self.build_query(data_str, table, buoy_id=buoy_id)
			self.write_to_db(query)

	def buoy_previous_years_dl(self, buoy_id: str, report_type: str = 'stdmet'):
		"""Downloads all data for every year previous to current
		
		Arguments:
			buoy_id: Identification number of the buoy
			report_type: Type of report that the buoy records, the types are listed in the README.
				Standard report type is Standard Meterological
		"""
		table = 'buoy_data'
		current_year = datetime.now().year
		for yr in range(current_year-1, 1900, -1):
			dl_url = self.buoy_base_url + f'data/historical/{report_type}/{buoy_id}h{yr}.txt.gz'
			data_str = self.get_site_content(dl_url)
			if data_str == 404:
				break
			query = self.build_query(data_str, table, buoy_id=buoy_id)
			self.write_to_db(query)
	
	def get_snow_station_data(self):
		"""Gets metadata for all snow stations & uploads to SQLite
		"""
		table = 'snow_stations'
		url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
		response = requests.get(url)
		data = response.text
		query = self.build_query(data, table)
		self.write_to_db(query)
			
	def get_site_content(self, dl_url: str) -> str:
		"""Fetches data from the given url

		Arguments:
			dl_url: the url to pull data from
		"""
		response = requests.get(dl_url)
		if response.status_code == 404:
			return 404
		content = gzip.decompress(response.content)
		data_str = content.decode('ascii')
		return data_str

	def build_query(self, data_str: str, table: str, **kwargs) -> str:
		"""Builds a query for inserting data into SQLite

		Arguments
			data_str: The data from the buoy in string format
			table: Name of the SQLite table to load the data
		
		Returns
			query: The query string to execute in SQLite
		"""
		# Build column names portion of query
		table_cols = [col for col in getattr(cfg, table).get('cols')]
		col_names = '('
		for col in table_cols:
			col_names += col + ','
		col_names = col_names[:len(col_names) - 1] + ")"

		# Build values portion of query
		data = data_str.split('\n')
		query_vals = ""
		if table == 'buoy_data':
			buoy_id = kwargs.get('buoy_id')
			if not buoy_id:
				raise Exception('Input error: User must supply a buoy ID to the `build_query()` method')
		else:
			buoy_id = None
		for row in data[:len(data) - 1]:
			row = self.parse_row(table, row, buoy_id=buoy_id)
			query_vals += row + ','

		# Remove last comma from the list
		query_vals = query_vals[:len(query_vals) - 1]

		query = f"""
		INSERT INTO {table} {col_names}
		VALUES
			{query_vals};
		"""
		return query
	
	def parse_row(self, table: str, row: str, **kwargs) -> str:
		"""Parses a row from raw string format to a format for loading to SQLite

		Arguments:
			table: the table to load the data to
			row: the raw string of the row
		"""
		if table == 'buoy_data':
			row = row.strip().split()
			buoy_id = kwargs.get('buoy_id')
			date = '-'.join(row[:3]) + ' ' + ':'.join(row[3:5]) + ':00'
			guid = buoy_id + re.sub('-|:| ', '', date)
			row.insert(0, guid)
			row.insert(0, buoy_id)
			row.insert(7, date)
		elif table == 'snow_stations':
			str_idx = cfg.snow_data_meta['station_col_idx']
			row = [row[str_idx[i]:str_idx[i+1]].strip() for i in range(len(str_idx) -1)]

		row = re.sub('\[', '(', str(row))
		row = re.sub('\]', ')', row)

		return row

	def write_to_db(self, query):
		"""Writes data to SQLite DB

		Arguments:
			query: The query to execute
		"""
		db_file = Path(__file__).parent.parent.parent / 'data/snowcast.db'
		with sqlite3.connect(db_file) as conn:
			cursor = conn.cursor()
			with open('sample.txt', 'w') as f:
				f.write(query)
			cursor.execute(query)


if __name__ == '__main__':
	dler = Downloader()
	# dler.buoy_current_year_dl('51001', 'stdmet')
	dler.get_snow_station_data()
