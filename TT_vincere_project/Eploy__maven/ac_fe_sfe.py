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
cf.read('maven_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
#
# from common import vincere_placement_detail
# import importlib
# importlib.reload(vincere_placement_detail)
# vpd = vincere_placement_detail.PlacementDetail(connection)
#
# assert False
# fe_sfe_mapping = pd.read_csv("fe_sfe.csv")
# vincere_custom_migration.inject_functional_expertise_subfunctional_expertise(fe_sfe_mapping, 'VC FE', fe_sfe_mapping, 'VC SFE', connection)

# %% load data
func = pd.read_sql("""
select link.* , cat.* from
(select RecordId as candidate_externalid, k.KeywordId
from KeywordRecordLink k
join Applicants c on k.RecordId = c.APP_ID) link
left join  (select kw.DICT_ID, kw.KEYWORD, kw.DEFINITION, kc.TYPE_NAME from Keywords kw
left join KeywordCategories kc on kc.CAT_ID = kw.TYPE) cat on link.KeywordId = cat.DICT_ID
""", engine_mssql)
# assert False

func = func.drop_duplicates()
func['fe'] = func['TYPE_NAME']
func['sfe'] = func['KEYWORD']
func['candidate_externalid'] = func['candidate_externalid'].astype(str)

from common import vincere_candidate
import importlib
importlib.reload(vincere_candidate)
vca = vincere_candidate.Candidate(connection)
func = func.loc[func.fe != '']
df = func

tem2 = df[['candidate_externalid', 'fe', 'sfe']]

tem2 = tem2.merge(pd.read_sql('select id as functional_expertise_id, name as fe from functional_expertise', vca.ddbconn), on='fe', how='left')
tem2 = tem2.merge(pd.read_sql('select functional_expertise_id, id as sub_functional_expertise_id, name as sfe from sub_functional_expertise', vca.ddbconn), on=['functional_expertise_id', 'sfe'], how='left')
tem2 = tem2.where(tem2.notnull(), None)
tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'] = tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'].astype(int)

tem2 = tem2.merge(vca.candidate, on=['candidate_externalid'])
tem2['candidate_id'] = tem2['id']
tem2['insert_timestamp'] = datetime.datetime.now()
tem2 = tem2.loc[tem2.functional_expertise_id.notnull()]
vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, vca.ddbconn, ['functional_expertise_id', 'candidate_id', 'insert_timestamp', 'sub_functional_expertise_id'], 'candidate_functional_expertise', mylog)
