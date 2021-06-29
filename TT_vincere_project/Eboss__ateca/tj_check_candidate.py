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
import datetime
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
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
assert False
cand = pd.read_sql("""select external_id, id, first_name, last_name, email from candidate where external_id is not null""", ddbconn)

cand_db = pd.read_sql("""SELECT CA.AttachmentID,c.[CandidateID], FirstName, Surname, CA.[AttachmentTypeID],CA.[Description],CA.[FilenameExtension],
  CAT.[CategoryDescription] AS [Category],ATT.[Description] AS [Type]
from Candidates c
join CandidateAttachments CA on c.CandidateID = ca.CandidateID
LEFT JOIN AttachmentTypes ATT ON ATT.[AttachmentTypeID] = CA.[AttachmentTypeID]
LEFT JOIN AttachmentTypeCategory CAT ON CAT.[CategoryID] = ATT.[CategoryID]""", engine_mssql)

cand_db_2 = pd.read_sql("""SELECT CandidateID
from Candidates c""", engine_mssql)
cand_db_2['CandidateID'] = cand_db_2['CandidateID'].astype(str)
assert False
cand['external_id'] = cand['external_id'].apply(lambda x: x.replace('FC',''))
cand_db['CandidateID'] =  cand_db['CandidateID'].astype(str)
cand_db_2['CandidateID'] =  cand_db_2['CandidateID'].astype(str)
cand_1 = cand.loc[~cand['external_id'].isin(cand_db['CandidateID'])]
cand_1 = cand_1.drop_duplicates()
cand_2 = cand.loc[~cand['external_id'].isin(cand_db_2['CandidateID'])]
cand_2 = cand_2.drop_duplicates()
cand_1.to_csv('tanjong_candidate_no_file.csv',index=False)
cand_2.to_csv('tanjong_candidate_not_in_fircroft.csv',index=False)

# %% check cand
cand_db = pd.read_sql("""SELECT CA.AttachmentID,CA.[CandidateID], CA.[AttachmentTypeID],CA.[Description],CA.[FilenameExtension],
  CAT.[CategoryDescription] AS [Category],ATT.[Description] AS [Type]
  FROM CandidateAttachments CA
  LEFT JOIN AttachmentTypes ATT ON ATT.[AttachmentTypeID] = CA.[AttachmentTypeID]
  LEFT JOIN AttachmentTypeCategory CAT ON CAT.[CategoryID] = ATT.[CategoryID]""", engine_mssql)
cand_db['CandidateID'] =  cand_db['CandidateID'].astype(str)

cand_db_1 = pd.read_sql("""select * from Candidates""", engine_mssql)
cand_db_1['CandidateID'] =  cand_db_1['CandidateID'].astype(str)

cand_csv = pd.read_sql("""
select RMSCANDIDATEID, "FIRST NAME", "LAST NAME", "HOME EMAIL" from Candidate
""", engine_sqlite) #59359

cand_1 = cand_db.loc[cand_db['CandidateID'].isin(cand_csv['RMSCANDIDATEID'])]
cand_1[['CandidateID']].drop_duplicates()


cand_2 = cand_csv.loc[cand_csv['RMSCANDIDATEID'].isin(cand_db_1['CandidateID'])]
cand_3 = cand_2.loc[~cand_2['RMSCANDIDATEID'].isin(cand_db['CandidateID'])]


cand_4 = cand_csv.loc[~cand_csv['RMSCANDIDATEID'].isin(cand_db_2['CandidateID'])]
cand_4.to_csv('tanjong_candidate_csv_not_in_fircroft.csv',index=False)







# %% extract data
# document_path_cand = r'D:\Tony\File\tanjong\CandidateAttachments (admin)\file'
# temp_msg_metadata_cand = vincere_common.get_folder_structure(document_path_cand)
# temp_msg_metadata_cand['matcher'] = temp_msg_metadata_cand['file'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
#
# cand_sql = pd.read_sql("""
# SELECT CA.AttachmentID,CA.[CandidateID],CA.[AttachmentTypeID],CA.[Description],CA.[FilenameExtension],
#   CAT.[CategoryDescription] AS [Category],ATT.[Description] AS [Type]
#   FROM CandidateAttachments CA
#   LEFT JOIN AttachmentTypes ATT ON ATT.[AttachmentTypeID] = CA.[AttachmentTypeID]
#   LEFT JOIN AttachmentTypeCategory CAT ON CAT.[CategoryID] = ATT.[CategoryID]
# """, engine_mssql)
# cand_sql['CandidateID'] = 'FC'+cand_sql['CandidateID'].astype(str)
# cand_sql['AttachmentID'] = cand_sql['AttachmentID'].astype(str)
# cand_sql['matcher'] = cand_sql['AttachmentID']+'.'+cand_sql['FilenameExtension']
# cand_sql['matcher'] = cand_sql['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
#
# temp_msg_metadata_cand = temp_msg_metadata_cand.merge(cand_sql, on='matcher')
# temp_msg_metadata_cand = temp_msg_metadata_cand.drop_duplicates()
# temp_msg_metadata_cand[['CandidateID']].drop_duplicates()