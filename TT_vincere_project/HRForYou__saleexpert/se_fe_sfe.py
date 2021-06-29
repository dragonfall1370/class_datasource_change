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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
review_db = cf[cf['default'].get('review_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()
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
select cand.*,
 ca1.adressgruppe_de as adressgruppe1,
 ca2.adressgruppe_de as adressgruppe2,
 ca3.adressgruppe_de as adressgruppe3
from
(select concat('KL',u.user_id) as candidate_externalid,
       u.nachname,
       u.vorname,
       p.adrgruppe1,
       p.adrgruppe2,
       p.adrgruppe3
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
where p.profil_id is not null) cand
left join cat_adressgruppe ca1 on ca1.id = cand.adrgruppe1
left join cat_adressgruppe2 ca2 on ca2.id = cand.adrgruppe2
left join cat_adressgruppe3 ca3 on ca3.id = cand.adrgruppe3
""", engine)

func = func.drop_duplicates()
func['fe'] = func[['adressgruppe1', 'adressgruppe2']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
func['sfe'] = func['adressgruppe3']
# keyword = keyword.merge(fe_sfe_mapping, left_on='name', right_on='Value in Evolve')
# keyword['fe'] = keyword['VC FE']
# keyword['sfe'] = keyword['VC SFE']
assert False
# %% contact fe
# contact_fe = keyword[['table_id', 'fe', 'sfe']]
# contact_fe['contact_externalid'] = contact_fe['table_id'].astype(str)
#
# from common import vincere_contact
# import importlib
# importlib.reload(vincere_contact)
# vc = vincere_contact.Contact(connection)
# cp0 = vc.insert_fe_sfe2(contact_fe, mylog)

# %% candidate fe
# candidate_fe = keyword[['table_id', 'fe', 'sfe']]
# candidate_fe['candidate_externalid'] = candidate_fe['table_id'].astype(str)


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
# cp1 = vca.insert_fe_sfe2(func, mylog)

# %% job fe
# job_fe = keyword[['table_id', 'fe', 'sfe']]
# job_fe['job_externalid'] = job_fe['table_id'].astype(str)
#
# from common import vincere_job
# import importlib
# importlib.reload(vincere_job)
# vcj = vincere_job.Job(connection)
# cp2 = vcj.insert_fe_sfe2(job_fe, mylog)
