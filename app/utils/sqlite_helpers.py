import sqlite3
from typing import Union
from os import PathLike
from pathlib import Path
from sqlite3 import Error
from collections import namedtuple
from config import table_dict

class SQLUtil:
    def __init__(self, db_path: Union[str, PathLike]):
        self.db_path = db_path


    def load_data(self, src: Union[str, PathLike], table: str):
        # TODO: create namedtuple to store & load data to SQLite
        pass

    def make_table(self, table: str):
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
