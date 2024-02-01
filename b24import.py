import yaml
from handlers import base, sql_handler, postgresql_handler, clickhouse_handler
from utils import get_data_from_databrics

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# MARK: MAPPERS
database_import_mapper = {
    'PostgreSQL': postgresql_handler.Handler,
    'ClickHouse': clickhouse_handler
}

# CONFIGURE
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
url = config['b24_key']

# get db type
database_type = config.get('database_type')
if database_type in database_import_mapper:
    handler: base.BaseHandler = database_import_mapper[database_type](
        conn_args=config['db_config']
    )
else:
    raise ValueError('Unsupported database type')

# check if tables are exist
for table in config['export_tables']:
    # create if not exist
    db_table = config['export_tables'][table]['db_table']
    if not handler.check_exist_table(db_table):
        handler.create_table(
            table=db_table, 
            column_spec=config[table]
        )
    del db_table


for table in config['export_tables']:
    # get data from bitrix24
    db_table = config['export_tables'][table]['db_table']
    method = config['export_tables'][table]['databrix_method']
    databrix_data = get_data_from_databrics(
        url=url, 
        method=config['export_tables'][table]['databrix_method'], 
        verbose=config['verbose'] if 'verbose' in config else False
    )
    # put data in db
    handler.load_data(
        table=db_table,
        column_spec=config[table],
        rows=databrix_data
    )
