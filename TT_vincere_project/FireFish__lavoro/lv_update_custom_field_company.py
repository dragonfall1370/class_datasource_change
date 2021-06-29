# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
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
cf.read('lv_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %% location
sql = """
select CompanyID, l.*
from CompanyOtherLocation cl
left join (select j1.id, j2.Value as Location, j1.Value as Location_Area from Drop_Down___Locations j1
left join Drop_Down___Locations j2 on j1.ParentID = j2.ID) l on cl.LocationWSIID = l.ID
"""
com = pd.read_sql(sql, engine_sqlite)
api = '15acd66efdae7ad783bcb7c993e6d401'
com['CompanyID'] = com['CompanyID'].apply(lambda x: str(x) if x else x)
com['value'] = com[['Location', 'Location_Area']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
com['value'] = com['value'].apply(lambda x: x.replace(' - New Item',''))
com = com.drop_duplicates()
# location = pd.read_sql("""
# select j2.Value as Location, j1.Value as Location_Area from Drop_Down___Locations j1
# left join Drop_Down___Locations j2 on j1.ParentID = j2.ID
# """, engine_sqlite)
# location['value'] = location[['Location', 'Location_Area']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
# location['value'] = location['value'].apply(lambda x: x.replace(' - New Item',''))
# location = location.drop_duplicates()
# tem = com.merge(location, on='value', suffixes=['', '_y'], how='outer', indicator=True)
# tem.loc[tem['_merge']=='right_only']
# tem['_merge'].unique()

vincere_custom_migration.insert_company_muti_selection_checkbox(com, 'CompanyID', 'value', api, connection)

# %% sttaus
sql = """
select c.ID, Value
from Company c
join Drop_Down___CompanyStatus cs on c.StatusWSIID = cs.ID
"""
com_stat = pd.read_sql(sql, engine_sqlite)
com_stat['ID'] = com_stat['ID'].apply(lambda x: str(x) if x else x)
api = '73e796a43f186af84733266fb8c1bd5c'

data = [['Customer', 'Customer'], ['Suspect', 'Suspect'], ['Partner / Competitor', 'Partner / Competitor']]
df = pd.DataFrame(data, columns=['ID', 'Value'])
com_stat = pd.concat([com_stat,df])
vincere_custom_migration.insert_company_drop_down_list_values(com_stat, 'ID', 'Value', api, connection)


