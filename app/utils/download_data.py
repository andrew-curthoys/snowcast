import requests
import chardet
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
		self.buoy_data_base = cfg.buoy_data_loc['base_site']

	def buoy_current_year(self, buoy_id: str, report_type: str = 'stdmet'):
		"""Method to download all data for the current year
		
		Arguments:
			buoy_id: Identification number of the buoy
			report_type: Type of report that the buoy records, the types are listed in the README.
				Standard report type is Standard Meterological
		"""
		table = 'buoy_data'
		for i, mth in enumerate(cfg.buoy_data_months()):
			mth_int = i + 1
			dl_path = self.buoy_data_base + f'data/{report_type}/{mth}/{buoy_id}{mth_int}{self.year}.txt.gz'
			data_str = self.get_site_content(dl_path)
			# response = requests.get(dl_path)
			# if response.status_code == 404:
			# 	break
			# content = gzip.decompress(response.content)
			# data_str = content.decode('ascii')
			query = self.build_query(data_str, buoy_id, table)
			self.write_to_db(query)

	def buoy_previous_years(self, buoy_id: str, report_type: str = 'stdmet'):
		"""Method to download all data for all years previous to current
		
		Arguments:
			buoy_id: Identification number of the buoy
			report_type: Type of report that the buoy records, the types are listed in the README.
				Standard report type is Standard Meterological
		"""
		table = 'buoy_data'
		current_year = datetime.now().year
		for yr in range(current_year-1, 1900, -1):
			dl_path = self.buoy_data_base + f'data/historical/{report_type}/{buoy_id}h{yr}.txt.gz'
			data_str = self.get_site_content(dl_path)
			if data_str == 404:
				break
			query = self.build_query(data_str, buoy_id, table)
			self.write_to_db(query)
			
	def get_site_content(self, dl_path: str) -> str:
		response = requests.get(dl_path)
		if response.status_code == 404:
			return 404
		content = gzip.decompress(response.content)
		data_str = content.decode('ascii')
		return data_str

	def build_query(self, data_str: str, buoy_id: str, table: str) -> str:
		"""Builds a query for inserting data into SQLite

		Arguments
			data_str: The data from the buoy in string format
			buoy_id: The ID number of the buoy
			table: name of the SQLite table to load the data
		
		Returns

		"""
		table_cols = [col for col in getattr(cfg, 'buoy_data').get('cols')]
		col_names = '('
		for col in table_cols:
			col_names += col + ','
		col_names = col_names[:len(col_names) - 1] + ")"
		data = data_str.split('\n')
		query_vals = ''
		for row in data[:len(data) - 1]:
			row = row.strip().split()
			date = '-'.join(row[:3]) + ' ' + ':'.join(row[3:5]) + ':00'
			guid = buoy_id + re.sub('-|:| ', '', date)
			row.insert(0, guid)
			row.insert(0, buoy_id)
			row.insert(7, date)
			row = re.sub('\[', '(', str(row))
			row = re.sub('\]', ')', row)
			query_vals += row + ','

		# Remove last comma from the list
		query_vals = query_vals[:len(query_vals) - 1]

		query = f"""
		INSERT INTO {table} {col_names}
		VALUES
			{query_vals};
		"""
		return query

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
	dler.buoy_previous_years('51001', 'stdmet')
