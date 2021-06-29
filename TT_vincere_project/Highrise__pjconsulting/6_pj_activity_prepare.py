# -*- coding: UTF-8 -*-
import configparser
import os
import pathlib

import pandas as pd
import psycopg2
import sqlalchemy

import common.logger_config as log
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('pj_config.ini')
mylog = log.get_info_logger(cf['default'].get('log_file'))
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect to database
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %% get comments
comp = pd.read_csv(os.path.join(standard_file_upload, 'company_activities.csv'))
cont = pd.read_csv(os.path.join(standard_file_upload, 'contact_activities.csv'))
job = pd.read_csv(os.path.join(standard_file_upload, 'jobs_activities_3.csv'))
cand = pd.read_csv(os.path.join(standard_file_upload, 'candidate_activities.csv'))

comp['insert_timestamp'] = pd.to_datetime(comp['Written'])
comp['owner'] = comp['Author']
comp = comp.where(comp.notnull(), None)
comp['content'] = comp[['Author', 'About', 'Subject', 'Body']]\
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Author', 'About', 'Subject', 'Body'], x) if e[1]]), axis=1)
comp.rename(columns={'company-externalId': 'company_external_id'}, inplace=True)

cont['insert_timestamp'] = pd.to_datetime(cont['Written'])
cont['owner'] = cont['Author']
cont = cont.where(cont.notnull(), None)
cont['content'] = cont[['Author', 'About', 'Subject', 'Body']]\
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Author', 'About', 'Subject', 'Body'], x) if e[1]]), axis=1)
cont.rename(columns={'contact-externalId': 'contact_external_id'}, inplace=True)

job['insert_timestamp'] = pd.to_datetime(job['Written'])
job['owner'] = job['Author']
job = job.where(job.notnull(), None)
job['content'] = job[['Author', 'About', 'Subject', 'Body']]\
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Author', 'About', 'Subject', 'Body'], x) if e[1]]), axis=1)
job.rename(columns={'company-externalId': 'company_external_id', 'contact-externalId': 'contact_external_id', 'candidate-externalId': 'candidate_external_id', 'ID': 'position_external_id'}, inplace=True)

cand['insert_timestamp'] = pd.to_datetime(cand['Written'])
cand['owner'] = cand['Author']
cand = cand.where(cand.notnull(), None)
cand['content'] = cand[['Author', 'About', 'Subject', 'Body']]\
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Author', 'About', 'Subject', 'Body'], x) if e[1]]), axis=1)
cand.rename(columns={'candidate-externalId': 'candidate_external_id'}, inplace=True)


# assert False
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(comp, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(cont, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(job, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(cand, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)

# re1.content = re1.content.map(lambda x: x.replace(temstr, ''))

# %% load to temp db
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
re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)





















