# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
import codecs
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('tt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
#dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""
assert False
# %% data connections
tmp ="curl -X GET https://api.catsone.com/v3/attachments/{0}/download -H 'authorization: Token 015d70eb577913fbf8168dfaacdf35ce' --output /thetalentdoc/{1}_{2}.{3}\nsleep 9"
file = pd.read_csv('candidates_attachments.csv')
file['extension'] = file['filename'].apply(lambda x: x.split('.')[-1])
file['download'] = file.apply(lambda x: tmp.format(x['id'],x['data_item.id'],x['id'],x['extension']), axis=1)
file1 = file[['download']]
file1.to_csv('download_file_talent.csv',index=False)

# %% company
tmp ="curl -X GET https://api.catsone.com/v3/attachments/{0}/download -H 'authorization: Token 015d70eb577913fbf8168dfaacdf35ce' --output /thetalentdoc_company/{1}_{2}.{3}\nsleep 9"
file = pd.read_csv('company_files.csv')
file['extension'] = file['filename'].apply(lambda x: x.split('.')[-1])
file['download'] = file.apply(lambda x: tmp.format(x['id'],x['data_item.id'],x['id'],x['extension']), axis=1)
file1 = file[['download']]
file1.to_csv('download_file_talent_compnay.csv',index=False)

# %% contact
tmp ="curl -X GET https://api.catsone.com/v3/attachments/{0}/download -H 'authorization: Token 015d70eb577913fbf8168dfaacdf35ce' --output /thetalentdoc_contact/{1}_{2}.{3}\nsleep 9"
file = pd.read_csv('contact_files.csv')
file['extension'] = file['filename'].apply(lambda x: x.split('.')[-1])
file['download'] = file.apply(lambda x: tmp.format(x['id'],x['data_item.id'],x['id'],x['extension']), axis=1)
file1 = file[['download']]
file1.to_csv('download_file_talent_contact.csv',index=False)

# %% job
tmp ="curl -X GET https://api.catsone.com/v3/attachments/{0}/download -H 'authorization: Token 015d70eb577913fbf8168dfaacdf35ce' --output /thetalentdoc_job/{1}_{2}.{3}\nsleep 9"
file = pd.read_csv('job_files.csv')
file['extension'] = file['filename'].apply(lambda x: x.split('.')[-1])
file['download'] = file.apply(lambda x: tmp.format(x['id'],x['data_item.id'],x['id'],x['extension']), axis=1)
file1 = file[['download']]
file1.to_csv('download_file_talent_job.csv',index=False)