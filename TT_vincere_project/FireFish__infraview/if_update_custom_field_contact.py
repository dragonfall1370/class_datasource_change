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
cf.read('if_config.ini')
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
# assert False
# %% contact
sql = """
select PersonID, Location, Location_Area  from ContactLocation c
left join (select j1.id, j2.Value as Location, j1.Value as Location_Area from Drop_Down___Locations j1
left join Drop_Down___Locations j2 on j1.ParentID = j2.ID) l on c.LocationWSIID = l.ID
"""
cont = pd.read_sql(sql, engine_sqlite)
cont['PersonID'] = cont['PersonID'].apply(lambda x: str(x) if x else x)
api = '4358854c80c01a33879fa5d3a101bce4'
cont['value'] = cont[['Location', 'Location_Area']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
cont['value'] = cont['value'].apply(lambda x: x.replace(' - Example Location',''))
vincere_custom_migration.insert_contact_muti_selection_checkbox(cont, 'PersonID', 'value', api, connection)

# %% job
job1 = pd.read_sql( """
select j.ID as job_externalid, LocationAreaWSIID, LocationWSIID, Location, Location_Area
from Job j
left join (select j1.id, j2.Value as Location, j1.Value as Location_Area from Drop_Down___Locations j1
left join Drop_Down___Locations j2 on j1.ParentID = j2.ID) l on j.LocationWSIID = l.ID
where LocationWSIID is not null
""", engine_sqlite)
job1['job_externalid'] = job1['job_externalid'].apply(lambda x: str(x) if x else x)
job2 = pd.read_sql( """
select j.ID as job_externalid, LocationAreaWSIID, LocationWSIID, l.Value as Location
from Job j
left join Drop_Down___Locations l on j.LocationAreaWSIID = l.ID
where LocationAreaWSIID is not null and LocationWSIID is null
""", engine_sqlite)
job2['job_externalid'] = job2['job_externalid'].apply(lambda x: str(x) if x else x)
job2['Location_Area'] = None
job = pd.concat([job1, job2])
job = job.drop_duplicates()
job['value'] = job[['Location', 'Location_Area']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
job['value'] = job['value'].apply(lambda x: x.replace(' - Example Location',''))


tem1 = pd.DataFrame(job['value'].value_counts().keys(), columns=['value'])
tem1['matcher'] = tem1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('location.csv')
tem2['matcher'] = tem2['Vincere value'].dropna().apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)

api = 'f04f7c3d866a349a246842250ef3df41'
vincere_custom_migration.insert_job_muti_selection_checkbox(job, 'job_externalid', 'value', api, connection)

# %% candidate
cand1 = pd.read_sql( """
select c.ID as candidate_externalid, LocationAreaWSIID, LocationWSIID, Location, Location_Area
from Candidate c
left join (select j1.id, j2.Value as Location, j1.Value as Location_Area from Drop_Down___Locations j1
left join Drop_Down___Locations j2 on j1.ParentID = j2.ID) l on c.LocationWSIID = l.ID
where LocationWSIID is not null
""", engine_sqlite)
cand1['candidate_externalid'] = cand1['candidate_externalid'].apply(lambda x: str(x) if x else x)
cand2 = pd.read_sql( """
select c.ID as candidate_externalid, LocationAreaWSIID, LocationWSIID, l.Value as Location
from Candidate c
left join Drop_Down___Locations l on c.LocationAreaWSIID = l.ID
where LocationAreaWSIID is not null and LocationWSIID is null
""", engine_sqlite)
cand2['candidate_externalid'] = cand2['candidate_externalid'].apply(lambda x: str(x) if x else x)
cand2['Location_Area'] = None
cand = pd.concat([cand1, cand2])
cand = cand.drop_duplicates()
cand['value'] = cand[['Location', 'Location_Area']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
cand['value'] = cand['value'].apply(lambda x: x.replace(' - Example Location',''))


tem1 = pd.DataFrame(cand['value'].value_counts().keys(), columns=['value'])
tem1['matcher'] = tem1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('location.csv')
tem2['matcher'] = tem2['Vincere value'].dropna().apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
api = 'dcdd4cb8f11a23c71c2b1e4a06d85038'
vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand, 'candidate_externalid', 'value', api, connection)


