# -*- coding: UTF-8 -*-
from pandas import DataFrame

from docx import Document
import common.logger_config as log
import configparser
import pathlib
import sqlalchemy
import re
import os
import pymssql
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)
#
# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('yc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
upload_folder = cf['default'].get('upload_folder')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre.raw_connection()

# %% query data and save to csv files
notes_file = pd.read_sql("""SELECT Id || '.docx' as file_name, Body as file_data FROM Note where Body is not null;""", engine_sqlite)
for index, row in notes_file.iterrows():
    # vincere_common.write_file(row['file_data'], '%s/%s' % (upload_folder, row['file_name']))
    # with open('%s/%s' % (upload_folder, row['file_name']), 'w') as f:
    #     f.write(row['file_data'])

    # pdf = FPDF()
    # pdf.add_page()
    # pdf.set_xy(0, 0)
    # pdf.set_font('arial', 'B', 13.0)
    # pdf.cell(ln=0, h=5.0, align='L', w=0, txt=row['file_data'], border=0)
    # pdf.output('%s/%s' % (upload_folder, row['file_name']), 'F')

    document = Document()
    document.add_paragraph(row['file_data'])
    document.save('%s/%s' % (upload_folder, row['file_name']))

