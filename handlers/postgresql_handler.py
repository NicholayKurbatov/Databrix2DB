from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine, inspect, text
from .base import BaseDBConnect, BaseHandler


class DBConnect(BaseDBConnect):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.pswd}@/{self.db}?host={self.host}:{self.port}"
        )
        self.session_maker = sessionmaker(
            engine, 
            class_=Session, 
            expire_on_commit=False, 
        )

    def get_session(self) -> Session:
        with self.session_maker() as session:
            yield session


class Handler(BaseHandler):
    def __init__(self, conn_args: dict[str, str | int]):
        self.session = DBConnect(**conn_args).get_session()

    def check_exist_table(self, table: str) -> bool:
        return inspect(self.session()).has_table(table)

    def create_table(self, table: str, column_spec: dict):
        column_settings = []
        for col in column_spec:
            col_str = (
                column_spec[col]['col'] + " " + 
                column_spec[col]['type'].upper()
            )
            if 'primary_key' in column_spec[col]:
                col_str += " PRIMARY KEY"
            column_settings.append(col_str)

        column_settings = "\n".join(column_settings)
        sql = text(f"""
            CREATE TABLE IF NOT EXIST {table} (
                {column_settings}
            )
        """)
        self.session.execute(sql)
        self.session.commit()

    def load_data(self, table: str, column_spec: dict, rows: list[dict]):
        columns = []
        primary_key = ""
        for col in column_spec:
            if 'primary_key' in column_spec[col]:
                primary_key = column_spec[col]['col']
            else:
                columns.append(column_spec[col]['col'])

        data = Handler.repack_data(column_spec, rows)

        sql = text(f"""
            INSERT INTO {table} ({','.join([primary_key] + columns)}) 
            VALUES {','.join(['%s'] * len(rows))}
            ON CONFLICT ({primary_key}) 
            DO UPDATE SET
            {', '.join(f'{key} = excluded.{key}' for key in columns)}
        """)
        self.session.execute(sql, data)
        self.session.commit()

    def delete_data(self, table: str, id_col: str, id: int):
        pass
