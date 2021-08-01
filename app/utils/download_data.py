import requests
import chardet
import config as cfg
import gzip
import os
import re
import sqlite3
import xml.etree.ElementTree as ET

from datetime import datetime
from pathlib import Path
from pykml import parser
from requests.models import Response
from io import BytesIO

class Downloader:
	def __init__(self):
		self.year = cfg.year
		self.buoy_base_url = cfg.buoy_data_meta['base_url']

	def buoy_dl_metadata(self, buoy_id=None):
		"""Downloads metadata for all buoys or a specific buoy if
		supplied
		
		Arguments:
			buoy_id: Identification number of a buoy if you'd like
			to add a specific buoy, otherwise it will download data
			for all buoys
		"""
		table = 'buoys'
		url = self.buoy_base_url + 'activestations.xml'
		response = requests.get(url)
		data_str = response.text
		query = self.build_query(data_str, table)
		self.write_to_db(query)

	def buoy_dl_current_year(self, buoy_id: str, report_type: str = 'stdmet'):
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

	def buoy_dl_previous_years(self, buoy_id: str, report_type: str = 'stdmet'):
		"""Downloads all data for every year previous to current
		
		Arguments:
			buoy_id: Identification number of the buoy
			report_type: Type of report that the buoy records, the types are listed in the README.
				Standard report type is Standard Meterological
		"""
		table = 'buoy_data'
		current_year = datetime.now().year
		for yr in range(current_year-1, 1900, -1):
			url = self.buoy_base_url + f'data/historical/{report_type}/{buoy_id}h{yr}.txt.gz'
			data_str = self.get_site_content(url)
			if data_str == 404:
				break
			query = self.build_query(data_str, table, buoy_id=buoy_id)
			self.write_to_db(query)
	
	def snow_dl_station_metadata(self):
		"""Gets metadata for all snow stations & uploads to SQLite
		"""
		table = 'snow_stations'
		url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
		response = requests.get(url)
		data = response.text
		query = self.build_query(data, table)
		self.write_to_db(query)
	
	def snow_dl_data(self, snow_station_id: str):
		"""Downloads data for a given snow station
		
		Arguments:
			station_id: The station ID of the snow station to download data from
		"""
		table = 'snow_data'
		url = f'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_station/{snow_station_id}.csv.gz'
		data_str = self.get_site_content(url)
		if data_str == 404:
			raise Exception('Error downloading file. Please check URL & snow station ID')
		query = self.build_query(data_str, table, snow_station_id=snow_station_id)
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
			data_str: The source data in string format
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

		# Get data as a list
		if table == 'buoys':
			data = ET.fromstring(data_str)
			start_row = 0
		else:
			data = data_str.split('\n')

		# Build values portion of query
		query_vals = ""
		kwarg_dict = {}
		if table == 'buoy_data':
			kwarg_dict['buoy_id'] = kwargs.get('buoy_id')
			start_row = 2
			if not kwarg_dict['buoy_id']:
				raise Exception('Input error: User must supply a buoy ID to the `build_query()` method')
		elif table == 'snow_data':
			kwarg_dict['snow_station_id'] = kwargs.get('snow_station_id')
			start_row = 0
			if not kwarg_dict['snow_station_id']:
				raise Exception('Input error: User must supply a snow station ID to the `build_query()` method')

		for row in data[start_row:len(data) - 1]:
			row = self.parse_row(table, row, kwarg_dict)
			if not row:
				continue
			query_vals += row + ','

		if table == 'buoys':
			last_row = data[len(data) - 1]
			row = self.parse_row(table, last_row, kwarg_dict)
			query_vals += row
		else:
			# Remove last comma from the list
			query_vals = query_vals[:len(query_vals) - 1]

		query = f"""
		INSERT OR REPLACE INTO {table} {col_names}
		VALUES
			{query_vals};
		"""
		return query
	
	def parse_row(self, table: str, row: str, kwarg_dict: dict) -> str:
		"""Parses a row from raw string format to a format for loading to SQLite

		Arguments:
			table: the table to load the data to
			row: the raw string of the row
			kwarg_dict: a dictionary of values to use while parsing depending on the table
		"""
		if table == 'buoy_data':
			row = row.strip().split()
			buoy_id = kwarg_dict.get('buoy_id')
			date = '-'.join(row[:3]) + ' ' + ':'.join(row[3:5]) + ':00'
			point_id = buoy_id + re.sub('-|:| ', '', date)
			row.insert(0, point_id)
			row.insert(0, buoy_id)
			row.insert(7, date)
		elif table == 'buoys':
			buoy_cols = [
				'id',
				'lat',
				'lon',
				'name',
				'owner',
				'pgm',
				'type',
				'met',
				'currents',
				'waterquality',
				'dart'
			]
			row = ['' if row.get(item) is None else row.get(item) for item in buoy_cols]
		elif table == 'snow_data':
			row = row.strip().split(',')
			if row[2] != 'SNOW':
				return
			point_id = row[0] + row[1]
			row.insert(1, point_id)
			date = row[2][:4] + '-' + row[2][4:6] + '-' + row[2][6:] + ' 00:00:00'
			row[2] = date
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
			cursor.execute(query)


if __name__ == '__main__':
	dler = Downloader()
	dler.buoy_dl_metadata()
	# dler.snow_dl_data('USC00420072')
	# dler.buoy_dl_current_year('51001', 'stdmet')
	# dler.buoy_dl_previous_years('51001', 'stdmet')
