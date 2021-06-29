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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('review_db')]
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

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(engine_postgre.raw_connection())

# %% job
placement_detail_info = pd.read_sql("""
select concat('SE',up.user_id) as candidate_externalid
, concat('SE',up.projekt_id) as job_externalid
, status_datum
, zielgehalt
, provision
 from user_projekte up
where status = 7
""", engine)
assert False
# %% start date
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'status_datum']].dropna()
tem['start_date'] = pd.to_datetime(tem['status_datum'])
tem['placed_date'] = pd.to_datetime(tem['status_datum'])
tem['offer_date'] = pd.to_datetime(tem['status_datum'])
cp2 = vplace.update_startdate_only_for_placement_detail(tem, mylog)
cp3 = vplace.update_placeddate(tem, mylog)
cp4 = vplace.update_offerdate(tem, mylog)

# %% curency
tem = placement_detail_info[['job_externalid', 'candidate_externalid']]
tem['currency_type'] = 'euro'
cp3 = vplace.update_offer_currency_type(tem, mylog)

# %% country
tem = placement_detail_info[['job_externalid', 'candidate_externalid']]
tem['country_code'] = 'DE'
cp3 = vplace.update_offer_country_code(tem, mylog)

# %% annual salry
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'zielgehalt']].dropna()
tem['annual_salary'] = tem['zielgehalt']
cp3 = vplace.update_offer_annual_salary(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'provision']].dropna()
tem['percentage_of_annual_salary'] = tem['provision']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)