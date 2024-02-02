import sys
sys.path.append('.')

import pytest
import yaml
from .templates import ready_data
from handlers import base, clickhouse_handler


# MARK: MAPPERS
database_import_mapper = {
    'ClickHouse': clickhouse_handler.Handler,
}

# CONFIGURE
with open('config.yaml', 'r', encoding="utf-8") as file:
    config = yaml.safe_load(file)
database_type = 'ClickHouse'
table = 'tasks'
config['db_config'] = dict(
  host='localhost',
  port=8123,
  user="user",
  password="user",
  db_name="db",
  ssl=None,
)
handler: base.BaseHandler = database_import_mapper[database_type](
    conn_args=config['db_config']
)
# check if tables are exist
db_table = config['export_tables'][table]['db_table']


def test_clickhouse_table_creation():
    if not handler.check_exist_table(db_table):
        # create if not exist
        handler.create_table(
            table=db_table, 
            column_spec=config[table]
        )
    assert handler.check_exist_table(db_table) == True


def test_clickhouse_insert_data():
    # put data in db
    handler.load_data(
        table=db_table,
        column_spec=config[table],
        rows=ready_data
    )
