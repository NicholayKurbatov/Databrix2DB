import psycopg2
from typing import Generator
from .base import BaseDBConnect, BaseHandler


class DBConnect(BaseDBConnect):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn_string = f"host='{self.host}' dbname='{self.db}' user='{self.user}' password='{self.pswd}' port={self.port}"
        # get a connection, if a connect cannot be made an exception will be raised here
        self.conn = psycopg2.connect(conn_string)

    def run_sql(self, sql, fetch=False):
        with self.conn.cursor() as session:
            session.execute(sql)
            res = None
            if fetch:
                res = session.fetchall()
        return res

    def insert_many(self, sql, data):
        with self.conn.cursor() as session:
            session.executemany(sql, data)
            self.conn.commit()


class Handler(BaseHandler):
    def __init__(self, conn_args: dict[str, str | int]):
        self.session = DBConnect(**conn_args)

    def check_exist_table(self, table: str) -> bool:
        sql = f"""
            SELECT EXISTS (
                SELECT FROM 
                    information_schema.tables 
                WHERE 
                    table_schema LIKE 'public' AND 
                    table_type LIKE 'BASE TABLE' AND
                    table_name = '{table}'
                );
        """
        return self.session.run_sql(sql, fetch=True)[0][0]

    def create_table(self, table: str, column_spec: dict):
        column_settings = []
        for col in column_spec:
            col_str = column_spec[col]['col'] + " "
            if column_spec[col]['type'] == 'string':
                col_str += 'varchar'
            elif column_spec[col]['type'] == 'datetime':
                col_str += 'TIMESTAMP'
            else:
                col_str += column_spec[col]['type']
            if 'primary_key' in column_spec[col]:
                col_str += " PRIMARY KEY"
            column_settings.append(col_str)

        column_settings = "\n,".join(column_settings)
        sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {column_settings}
            )
        """
        self.session.run_sql(sql)

    def load_data(self, table: str, column_spec: dict, rows: list[dict]):
        columns = []
        primary_key = ""
        for col in column_spec:
            if 'primary_key' in column_spec[col]:
                primary_key = column_spec[col]['col']
            else:
                columns.append(column_spec[col]['col'])

        data = Handler.repack_data(column_spec, rows)
        print(data)

        sql = f"""
            INSERT INTO {table} ({','.join([primary_key] + columns)}) 
            VALUES ({','.join(['%s'] * len(data[0]))})
            ON CONFLICT ({primary_key}) 
            DO UPDATE SET
            {', '.join(f'{key} = excluded.{key}' for key in columns)}
        """
        print(sql)
        self.session.insert_many(sql, data)

    def delete_data(self, table: str, id_col: str, id: int):
        pass
