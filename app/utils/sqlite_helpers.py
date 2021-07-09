from utils.config import table_dict
from os import PathLike
from pandas import read_sql_query
from pathlib import Path
from typing import Union
from collections import namedtuple
from csv import reader as csv_reader
import sqlite3
from sqlite3 import Error

class SQLUtil:
    def __init__(self, db_path: Union[str, PathLike]):
        self.db_path = db_path
        self.data = []


    def load_data(self, table: str):
        # TODO: build SQL insert statement for inserting data
        pass
    
    def read_data(self, query: str):
        """
        Read data from a db & return it as a Pandas df

        Args:
            query: query to run & return data
        """
        with sqlite3.connect(self.db_path) as conn:
            df = read_sql_query(query, conn)
            return df

    def make_table(self, table: str):
        """
        Creates a table in the SQLite database

        Args:
            table: the name of the table to create
        """
        col_data = table_dict[table]['cols']
        query = f'CREATE TABLE IF NOT EXISTS {table} ('
        stmt = []

        for (key, value) in col_data.items():
            stmt.append(f'{key} {value}')
        try:
            supp_data = table_dict[table]['supplemental_info']
            stmt.append(supp_data)
        except KeyError:
            pass
        
        stmt_str = ', '.join(stmt)
        query = query + stmt_str + ');'

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

if __name__ == '__main__':
    sql_util = SQLUtil('/home/andrew-curthoys/Documents/Projects/snowcast/data/snowcast.db')
    sql_util.make_table('snow_stations')