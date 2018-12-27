# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import pymssql
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
from common import thread_pool as thread_pool
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)
#
# loading configuration
cf = configparser.RawConfigParser()
cf.read('_edenscott_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
locator = os.path.join(data_folder, 'locator')
to = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
pathlib.Path(locator).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')

    # pd.read_sql('select address from common_location where longitude is null and address is not null;', ddbconn).to_csv('candidate_address_no_lnglat.csv')
    # pd.read_sql('select address from company_location where longitude is null and address is not null;', ddbconn).to_csv('company_address_no_lnglat.csv')
    df_cand = pd.read_sql("""
    select a.id, a.external_id, a.first_name, middle_name, last_name, a.email, a.country, cl.post_code, cl.address, a.country
from candidate a
left join common_location cl on a.current_location_id = cl.id
    """, ddbconn)

    df_comp = pd.read_sql("""
    select a.id, a.external_id, a.name, a.website, cl.address, cl.post_code, cl.city, cl.state, cl.country from company a left join company_location cl on a.id = cl.company_id;
    """, ddbconn)

    df_cand_job = pd.read_sql("""
    select candidate_id, current_job_title from candidate_extension;
    """, ddbconn)

    df_cand.to_csv(r'D:\edenscott_cand.csv', index=False)
    df_comp.to_csv(r'D:\edenscott_comp.csv', index=False)
    df_cand_job = df_cand_job.loc[df_cand_job['current_job_title'].notnull(), ]
    df_cand_job.to_csv(r'D:\cand_job_current_jobtitle.csv', index=False)

