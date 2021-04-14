from sqlite_helpers import SQLUtil
from pathlib import Path


def make_tables(db_path):
    sql_util = SQLUtil(db_path)
    db_tables = ['buoy_data', 'buoys', 'snow_data', 'snow_stations']
    for table in db_tables:
        sql_util.make_table(table)
        print(f'{table} table created')
