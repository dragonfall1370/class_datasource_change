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
cf.read('tj_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(connection)
# %% permanent
place = pd.read_sql("""
select CandidateID, EnquiryID
from Placements2 p
left join PlacementStatus2 ps on p.StatusID = ps.StatusID
where CandidateID is not null and EnquiryID is not null
and ps.Status in ('Termination','Did Not Start','Perm Terminated','Perm Did Not Start')
""", engine_mssql)
place['candidate_externalid'] = 'FC'+place['CandidateID'].astype(str)
place['job_externalid'] = 'FC'+place['EnquiryID'].astype(str)

invoice = pd.read_sql("""
select i.id
        , pd.external_id as job_externalid
        , c.external_id as candidate_externalid
        from position_candidate pc
        left join invoice i on pc.id = i.position_candidate_id
        left join position_description pd on pc.position_description_id = pd.id
        left join candidate c on pc.candidate_id = c.id
""", connection)
place = place.merge(invoice, on=['job_externalid', 'candidate_externalid'])
place['id'] = place['id'].apply(lambda x: int(str(x).split('.')[0]))
place['renewal_flow_status'] = 2
vincere_custom_migration.psycopg2_bulk_update_tracking(place, vplace.ddbconn, ['renewal_flow_status', ], ['id', ], 'invoice', mylog)
