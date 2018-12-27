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
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
pathlib.Path(locator).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)

    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')

    df_candidate_file = pd.read_sql(
            """
            select 
                ltrim(rtrim(Candidate)) as candidate_external_id
                , case when PayCurrency is not null and PayCurrency !='' then PayCurrency else 'GBP' end as cn_present_salary_currency 
            from IntControl;
            """
            , sdbconn
            )
    df_vincere_candidate = pd.read_sql("""select id, external_id from candidate""", ddbconn)
    df_candidate_file = df_candidate_file.merge(df_vincere_candidate, left_on='candidate_external_id', right_on='external_id')
    df_candidate_file['cn_present_salary_currency'] = df_candidate_file['cn_present_salary_currency'].apply(lambda x: vincere_common.map_currency_code(x))
if False:
    list_values = []
    for index, row in df_candidate_file.iterrows():
        a_record = list()
        a_record.append(row['id'])
        a_record.append(row['cn_present_salary_currency'])
        list_values.append(
            tuple(a_record)
        )



    cur = ddbconn.cursor()
    sql = """
        update candidate set currency_type=data.v1 from (values %s) as data(id, v1)
        where candidate.id=data.id 
          """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    cur.execute("""
    update candidate set currency_type='gbp' where id in (
    select id from candidate where currency_type='vnd'
    )
    ;
    """
    )
    ddbconn.commit()
    cur.close()