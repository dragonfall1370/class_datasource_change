# -*- coding: UTF-8 -*-
import configparser
import os
import pathlib

import pandas as pd
import psycopg2
import sqlalchemy

import common.logger_config as log
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common

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
# src_db = cf[cf['default'].get('src_db')]
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

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

# %% comment
activities = pd.read_sql("""
select a.id
     , com.id as company_external_id
     , cont.id as contact_external_id
     , j.id as position_external_id
     , cand.id as candidate_external_id
     , a.date_created as insert_timestamp
     , a.notes
     , u.username as owner
from activities a
left join users u on a.entered_by_id = u.id
left join company com on a."data_item.id" = com.id
left join contact cont on a."data_item.id" = cont.id
left join jobs j on a."data_item.id" = j.id
left join candidates cand on a."data_item.id" = cand.id
""", engine_sqlite)
activities = activities.drop_duplicates()
# assert False
# %% transform data
# feed['body'] = feed['body'].map(lambda x: html_to_text(x) if x else x)
# feed['content'] = feed[['template_name', 'subject', 'body', 'notes']]\
#     .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Type', 'Subject', 'Body', 'Quick notes'], x) if e[1]]), axis=1)

activities['content'] = activities[['notes']].apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Notes'], x) if e[1]]), axis=1)

# activities['entity_type'].unique()
# feed['insert_timestamp'] = pd.to_datetime(feed['insert_timestamp'])
activities['insert_timestamp'] = pd.to_datetime(activities['insert_timestamp'])
# %% load to temp db

from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(activities, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
# re2 = vincere_activity.transform_activities_temp(note, conn_str_ddb, mylog)
# re2 = re2.where(re2.notnull(), None)
dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
# re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)

























