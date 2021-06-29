# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('at_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

def html_to_text(html):
    from bs4 import BeautifulSoup
    # url = "http://news.bbc.co.uk/2/hi/health/2284783.stm"
    # html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html)

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text
# assert False
# %% pending
activity = pd.read_sql("""
select * from notes
""", engine_mssql)
activity = activity.drop_duplicates()
# activity = activity.where(activity.notnull(),None)
activity['candidate_external_id'] = activity['uid'].apply(lambda x: 'AT'+str(x) if x else x)
activity['contact_external_id'] = activity['uid'].apply(lambda x: 'AT'+str(x) if x else x)
# activity['contact_external_id'] = activity['ContactID'].apply(lambda x: 'FC'+str(x).split('.')[0] if x else x)
# activity['company_external_id'] = activity['ClientID'].apply(lambda x: 'FC'+str(x).split('.')[0] if x else x)
# activity['matcher'] = activity['CreatedBy'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
assert False
# %%
activity['insert_timestamp'] = pd.to_datetime(activity['onaddedon'], format='%Y/%m/%d %H:%M:%S')
activity['owner'] = ''
activity['content'] = activity['oncontent']
activity = activity.loc[activity['oncontent'].notnull()]
# activity['content'] = activity['content'].apply(lambda x: x.replace('"<','<').replace('>"','>'))

# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(activity, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)

dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR,
    'type': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity_prod', if_exists='replace', dtype=dtype, index=False)

# %% activity
# activity = pd.read_sql("""
# select id, company_id, contact_id, content, insert_timestamp, user_account_id from activity where company_id is not null and contact_id is not null
# """, engine_postgre_review)
# activity['matcher'] = activity['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# re3['matcher'] = re3['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem = activity.merge(re3, on=['company_id','contact_id','matcher','insert_timestamp'])
# tem2 = tem[['id','position_id']].drop_duplicates()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['position_id', ], ['id', ], 'activity', mylog)