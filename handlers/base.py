from abc import abstractmethod, ABC
from typing import Generator
from ssl import create_default_context, CERT_REQUIRED
from utils import type_caster


class BaseSession(ABC):
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def execute(self, query: str):
        pass

    @abstractmethod
    def commit(self):
        pass


class BaseDBConnect(object):
    def __init__(self, host: str, port: int, user: str, password: str, db_name: str, ssl: str = None) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.pswd = password
        self.db = db_name
        self.ssl = ssl
        if self.ssl is not None:
            self.ssl = create_default_context(
                cafile=self.ssl
            )
            self.ssl.verify_mode = CERT_REQUIRED
            self.ssl.check_hostname = True

    def get_session() -> Generator[BaseSession, None, None]:
        pass


class BaseHandler(ABC):
    def __init__(self, conn_args: dict[str, str | int]):
        self.session = BaseDBConnect(**conn_args).get_session()
    
    @abstractmethod
    def check_exist_table(self, table: str) -> bool:
        pass

    @abstractmethod
    def create_table(self, table: str, column_spec: dict):
        pass

    @abstractmethod
    def load_data(self, table: str, column_spec: dict, rows: list[dict]):
        pass

    @abstractmethod
    def delete_data(self, table: str, id_col: str, id: int):
        pass

    @staticmethod
    def repack_data(column_spec: dict, rows: list[dict]) -> list[dict]:
        data = []
        for row in rows:
            db_row = []
            for col in column_spec:
                if col not in row:
                    db_row.append(None)
                    continue
                db_row.append(
                   type_caster(row[col], column_spec[col]['type'])
                )
            data.append(tuple(db_row))
        return data