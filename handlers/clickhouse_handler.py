import clickhouse_connect
from tqdm import tqdm
import time
from datetime import datetime
from .base import BaseDBConnect, BaseHandler
from utils import type_caster


class DBConnect(BaseDBConnect):
    def __init__(self, *args, https: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        # get a connection, if a connect cannot be made an exception will be raised here
        self.conn = clickhouse_connect.get_client(
            interface='https' if https else 'http', 
            host=self.host, 
            port=self.port, 
            username=self.user,
            password=self.pswd, 
            database=self.db
        )

    def run_sql(self, sql: str, **kwargs):
        res = self.conn.command(sql)
        return res

    def insert_many(self, table: str, data=list[tuple], column_names=list[str]):
        self.conn.insert(table, data, column_names)


class Handler(BaseHandler):
    def __init__(self, conn_args: dict[str, str | int]):
        self.session = DBConnect(**conn_args)

    def check_exist_table(self, table: str) -> bool:
        return bool(self.session.run_sql(f"""
            EXISTS TABLE {table}
        """))

    def create_table(self, table: str, column_spec: dict):
        column_settings = []
        primary_key = ""
        for col in column_spec:
            col_str = column_spec[col]['col'] + " "
            if 'string' in column_spec[col]['type']:
                col_str += "String NULL"
            elif '[]' in column_spec[col]['type']:
                col_str += 'Array(Nullable(' + column_spec[col]['type'].split('[]')[0] + '))'
            elif 'primary_key' in column_spec[col]:
                primary_key = column_spec[col]['col']
                col_str += column_spec[col]['type'] + " NOT NULL"
            else:
                col_str += column_spec[col]['type'] + " NULL"
            
            column_settings.append(col_str)

        column_settings = ",\n".join(column_settings)
        sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {column_settings}
            )
            ENGINE = ReplacingMergeTree
            ORDER BY ({primary_key})
        """
        print(sql)
        self.session.run_sql(sql)

    def load_data(self, table: str, column_spec: dict, rows: list[dict]):
        columns = [column_spec[col]['col'] for col in column_spec]
        data = Handler.repack_data(column_spec, rows)
        self.session.insert_many(
            table=table, 
            data=data,
            column_names=columns
        )
        self.session.run_sql(f"OPTIMIZE TABLE {table}")

    def delete_data(self, table: str, id_col: str, id: int):
        pass

    @staticmethod
    def repack_data(column_spec: dict, rows: list[dict]) -> list[dict]:
        data = []
        for row in rows:
            db_row = []
            for col in column_spec:
                if col not in row:
                    if '[]' in column_spec[col]['type']:
                        db_row.append([None,])
                    else:
                        db_row.append(None)
                    continue
                db_row.append(
                   type_caster(row[col], column_spec[col]['type'])
                )
            data.append(tuple(db_row))
        return data
