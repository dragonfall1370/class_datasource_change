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

# %% extract data
engine_sqlite = sqlalchemy.create_engine('sqlite:///salesexpert.db', encoding='utf8')

connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

# %%
reminder = pd.read_sql("""
select ativities.*, l.reg_mail
from
(select a.referenz_id as candidate_external_id,
        a.kennung,
       a.art_id as cat_id,
       a.erledigt_am as create_date,
       p.titel,
       a.betreff as subject,
       a.beschreibung as description,
       a.user_id as owner
from aktionen a
left join projekte p on a.projekt_id = p.id) ativities
left join user_login l on ativities.owner = l.user_id
left join user_data u on ativities.candidate_external_id = u.user_id
where ativities.kennung = 4
""", engine)
reminder = reminder.drop_duplicates()
reminder
assert False
# %% transform data

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
# reminder.loc[reminder.candidate_external_id == 11]
reminder = reminder.loc[reminder.create_date != '0000-00-00 00:00:00']
reminder['insert_timestamp'] = pd.to_datetime(reminder['create_date'])
reminder = reminder.merge(pd.read_sql("select id, text_erledigt as cat_type from cat_aktionen_arten where locale = 'de_DE' and kennung in(0,4)", engine.raw_connection()), left_on='cat_id', right_on='id', how='left')
# reminder.loc[reminder.candidate_external_id == 11]
reminder['owner'] = reminder['reg_mail']


reminder['description'] = reminder['description'].apply(lambda x: html_to_text(x))
reminder['cat_type'] = reminder['cat_type'].where(reminder['cat_type'].notnull(), 'keine Angabe')
reminder['content'] = reminder[['cat_type', 'titel', 'subject', 'description']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Art', 'Projekte', 'Betreff', 'Beschreibung'], x) if e[1]]), axis=1)
reminder.loc[46]
# reminder.to_csv('activities_comments.csv')


# %% load to temp db

from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))
re2 = vincere_activity.transform_activities_temp(reminder, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
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
re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
# re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)

























