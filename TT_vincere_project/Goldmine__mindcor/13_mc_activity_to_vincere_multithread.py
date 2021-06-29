# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
from functools import reduce
from common import vincere_job_application
import pandas as pd

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('mc_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% write activity to temp_tung_activity
def writedb(df, dtype, table_name, connection_str, idx):
    for attempt in range(10):
        try:
            mylog.info("Writing to %s rows %s" % (table_name, len(df)))
            eng = sqlalchemy.create_engine(connection_str, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
            df.to_sql(con=eng, name=table_name, if_exists='replace', chunksize=len(df), dtype=dtype, index=False)
        except Exception as oe:
            print(oe)
            print("Sleep 60s before attempt to reinsert: {}".format(attempt))
            time.sleep(60)
        else:
            break
    else:  # fail after 10 times attemt
        # df.to_csv('20190328_insert_error_{}_{}.csv'.format(table_name, idx), index=False)
        df.to_sql(con=engine_sqlite, name=table_name, if_exists='replace', chunksize=len(df), index=False)
    eng.dispose()


# %% get activity
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
activity = pd.read_sql("select * from vincere_activity", engine_sqlite)
mylog.info('number of rows will be inserted: %i' % len(activity))
dfs = vincere_common.df_split_to_listofdfs(activity, 1000)

# %% load to vincere
from common import thread_pool
pool = thread_pool.ThreadPool(50)
tbls = []
error_tbls = []

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))

sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
current_activity_id = pd.read_sql("select max(id) as current_activity_id from activity", sdbconn_engine)
for _, df in enumerate(dfs):
    df = df.where(df.notnull(), None)
    df['user_account_id'].fillna(-10, inplace=True)

    # tbl = 'temp_{0}{1}'.format(table_name, _)
    tbls.append('ztung_tem_activity_{}'.format(_))
    pool.add_task(writedb, df, vincere_common.sqlcol(activity, is_postgre=True), 'ztung_tem_activity_{}'.format(_), conn_str_ddb, _)
pool.wait_completion()

sql = """
INSERT INTO activity (company_id, contact_id, candidate_id, position_id, user_account_id, insert_timestamp, content, category)
select cast(company_id as int), cast(contact_id as int), cast(candidate_id as int), cast(position_id as int), cast(user_account_id as int), TO_TIMESTAMP(insert_timestamp, 'YYYY-MM-DD hh24:mi:ss'), content, category from {}
    """

for t in tbls:
    for attemp in range(0, 10):
        try:
            mylog.info(t)
            sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
            connection = sdbconn_engine.raw_connection()
            vincere_custom_migration.execute_sql_update(sql.format(t), connection)
            connection.close()
            sdbconn_engine.dispose()
        except:
            time.sleep(30)
            sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
            connection = sdbconn_engine.raw_connection()
            pass
        else:
            break
    else: # fail after 10 times attempt
        error_tbls.append(t)
assert 0==len(error_tbls)
# assert False

from common import vincere_activity
vincere_activity.map_activities_to_entities(conn_str_ddb, 17)

sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
connection = sdbconn_engine.raw_connection()
vincere_custom_migration.execute_sql_update("update activity set content=replace(content, '\r\n', chr(10)) where content is not null", connection)

# %% remove tem table
for i in range(0, len(dfs)):
    try:
        t = 'ztung_tem_activity_{}'.format(i)
        vincere_custom_migration.execute_sql_update('drop table {}'.format(t), connection)
    except:
        pass