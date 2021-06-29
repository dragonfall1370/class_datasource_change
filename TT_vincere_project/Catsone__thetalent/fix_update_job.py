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
import datetime
from pandas.io.json import json_normalize
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
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

from common import vincere_job
vjob = vincere_job.Job(engine_postgre.raw_connection())

def html_to_text(html):
    from bs4 import BeautifulSoup
    # url = "http://news.bbc.co.uk/2/hi/health/2284783.stm"
    # html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html)

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text

# %% reg date

# job = pd.read_sql("""
# select j.id as job_externalid
# , "_embedded.status.mapping" as status
# from jobs_new j
# """, engine_sqlite)
# job['job_externalid'] = job['job_externalid'].astype(str)
# assert False
#
# # %% open/close  start date end date
# tem = job[['job_externalid', 'status']].dropna()
# tem1 = tem.loc[tem['status'] == 'closed']
# tem2 = tem.loc[tem['status'] == 'filled']
# tem3 = pd.concat([tem1, tem2])
#
# tem3['close_date'] = datetime.datetime(2020, 2, 27)
# tem3['close_date'] = pd.to_datetime(tem3['close_date'])
# cp1 = vjob.update_close_date(tem3, mylog)


job = pd.read_sql("""
select pd.id, pd.external_id as job_externalid,  head_count_close_date ,placed_date from position_description pd
join (select offer.placed_date
     , pos_cand.position_description_id
from position_candidate pos_cand
join (select i.position_candidate_id, opi.placed_date
from invoice i join offer_personal_info opi on opi.offer_id = i.offer_id) offer on offer.position_candidate_id = pos_cand.id ) pc on pc.position_description_id = pd.id
where pd.external_id is not null
""", engine_postgre)
assert False
job['close_date'] = job['placed_date']
vjob.update_close_date(job,mylog)