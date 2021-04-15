from config import table_dict
from os import PathLike
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
    
    def read_data(self, file_path: Union[str, PathLike], table: str):
        """
        Read data from a file & store in a data structure

        Args:
            file_path: file path for the data to be read
            table: table where the data will be loaded
        """
        # Create a named tuple to store the data to upload into a table 
        DataRow = namedtuple('DataRow', [key for key in table_dict[table]['cols']])
        with open(file_path, newline='') as f:
            reader = csv_reader(f, delimiter=',')
            for row in reader:
                data_row = DataRow(row)
                self.data.append(data_row)

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
