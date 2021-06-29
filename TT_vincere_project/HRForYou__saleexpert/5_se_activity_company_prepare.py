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

# %%
company = pd.read_sql("""
select ativities.*, l.reg_mail
from
(select a.referenz_id as company_external_id,
        a.kennung,
       caa.text_erledigt as title,
       a.erledigt_am as create_date,
       p.titel,
       a.betreff as subject,
       a.beschreibung as description,
       a.user_id as owner
from aktionen a
left join projekte p on a.projekt_id = p.id
left join (select id, text_erledigt from cat_aktionen_arten where kennung = 1 and locale = 'de_DE') caa on caa.id = a.art_id) ativities
left join user_login l on ativities.owner = l.user_id
left join mand_firma m on ativities.company_external_id = m.firma_id
where ativities.kennung = 1
""", engine)
company = company.drop_duplicates()
assert False
# %% transform data
company = company.loc[company.create_date != '0000-00-00 00:00:00']
company['insert_timestamp'] = pd.to_datetime(company['create_date'])
# reminder.loc[reminder.candidate_external_id == 11]
company['owner'] = company['reg_mail']


company['description'] = company['description'].apply(lambda x: html_to_text(x))
company['title'] = company['title'].where(company['title'].notnull(), 'keine Angabe')
company['content'] = company[['title', 'description']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Titel', 'Art'], x) if e[1]]), axis=1)
# reminder.to_csv('activities_comments.csv')

# %% load to temp db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))
re1 = vincere_activity.transform_activities_temp(company, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
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
re1.to_sql(con=engine_sqlite, name='vincere_company_activity', if_exists='replace', dtype=dtype, index=False)

























