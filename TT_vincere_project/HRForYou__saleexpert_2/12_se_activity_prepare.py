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
# assert False
# %%
company = pd.read_sql("""
select ativities.*, l.reg_mail, concat(l2.vorname,' ', l2.nachname) as created_by
from
(select a.referenz_id as company_external_id,
        a.person_id as contact_external_id,
        a.kennung, a.erstellt_user_id,
       caa.text_erledigt as title,
       a.erledigt_am as create_date, a.erstellt_am,
       p.titel,
       a.betreff as subject,
       a.beschreibung as description,
       a.user_id as owner
from aktionen a
left join projekte p on a.projekt_id = p.id
left join (select id, text_erledigt from cat_aktionen_arten where kennung = 1 and locale = 'de_DE') caa on caa.id = a.art_id) ativities
join mand_firma m on ativities.company_external_id = m.firma_id
left join user_login l on ativities.owner = l.user_id
left join user_data l2 on ativities.erstellt_user_id = l2.user_id
where ativities.kennung = 1
""", engine)
company = company.drop_duplicates()
company['company_external_id'] = company['company_external_id'].astype(str)
company['contact_external_id'] = company['contact_external_id'].astype(str)
company['company_external_id'] = 'SE'+company['company_external_id']
company['contact_external_id'] = 'SE'+company['contact_external_id']

task = pd.read_sql("""
select ativities.*, l.reg_mail, concat(l2.vorname,' ', l2.nachname) as created_by
from
(select a.referenz_id as company_external_id,
        a.person_id as contact_external_id,
        a.kennung, a.erstellt_user_id,
       caa.text_erledigt as title,
       a.erledigt_am as create_date, a.erstellt_am, faellig_am,
       p.titel,
       a.betreff as subject,
       a.beschreibung as description,
       a.user_id as owner
from aktionen a
left join projekte p on a.projekt_id = p.id
left join (select id, text_erledigt from cat_aktionen_arten where kennung = 1 and locale = 'de_DE') caa on caa.id = a.art_id) ativities
join mand_firma m on ativities.company_external_id = m.firma_id
left join user_login l on ativities.owner = l.user_id
left join user_data l2 on ativities.erstellt_user_id = l2.user_id
where ativities.kennung = 1
and create_date = '0000-00-00'
""", engine)
task = task.drop_duplicates()
task['company_external_id'] = task['company_external_id'].astype(str)
task['contact_external_id'] = task['contact_external_id'].astype(str)
task['company_external_id'] = 'SE'+task['company_external_id']
task['contact_external_id'] = 'SE'+task['contact_external_id']

job = pd.read_sql("""
select ativities.*, l.reg_mail, p1.titel,concat(l2.vorname,' ', l2.nachname) as created_by
from
(select a.id,
        a.projekt_id as position_external_id,
        a.referenz_id as candidate_external_id,
       caa.text_erledigt as title,a.erstellt_user_id,
       a.erledigt_am as create_date, a.erstellt_am,
       a.betreff as subject,
       a.beschreibung as description,
       a.user_id as owner
from aktionen a
left join (select id, text_erledigt from cat_aktionen_arten where locale = 'de_DE') caa on caa.id = a.art_id) ativities
join projekte p1 on ativities.position_external_id = p1.id
left join user_login l on ativities.owner = l.user_id
left join user_data l2 on ativities.erstellt_user_id = l2.user_id
""", engine)
job = job.drop_duplicates()
job['position_external_id'] = job['position_external_id'].astype(str)
job['candidate_external_id'] = job['candidate_external_id'].astype(str)
job['position_external_id'] = 'SE'+job['position_external_id']
job['candidate_external_id'] = 'SE'+job['candidate_external_id']

cand = pd.read_sql("""
select ativities.*, l.reg_mail, concat(l2.vorname,' ', l2.nachname) as created_by
from
(select a.id,
a.referenz_id as candidate_external_id,
        a.kennung, a.erstellt_user_id,
        caa.text_erledigt as title,
       a.erledigt_am as create_date, a.erstellt_am,
       p.titel,
       a.betreff as subject,
       a.beschreibung as description,
       a.user_id as owner
from aktionen a
left join projekte p on a.projekt_id = p.id
left join (select id, text_erledigt from cat_aktionen_arten where kennung in(0,4) and locale = 'de_DE') caa on caa.id = a.art_id) ativities
join user_data u on ativities.candidate_external_id = u.user_id
left join user_login l on ativities.owner = l.user_id
left join user_data l2 on ativities.erstellt_user_id = l2.user_id
where ativities.kennung = 4
""", engine)
cand = cand.drop_duplicates()
cand['candidate_external_id'] = cand['candidate_external_id'].astype(str)
cand['candidate_external_id'] = 'SE'+cand['candidate_external_id']
cand = cand.loc[~cand['id'].isin(job['id'])]
assert False
# %% transform data
company = company.loc[company.create_date != '0000-00-00 00:00:00']
company['insert_timestamp'] = pd.to_datetime(company['create_date'])
company['owner'] = company['reg_mail']
company['description'] = company['description'].apply(lambda x: html_to_text(x))
company['title'] = company['title'].where(company['title'].notnull(), 'Email')
company['subject'] = company['subject'].where(company['subject'] != '', None)
company['description'] = company['description'].where(company['description'] != '', None)
company['erstellt_am'] = company['erstellt_am'].astype(str)
company['content'] = company[['title','titel','subject','description', 'erstellt_am', 'created_by']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Art','Titel', 'Betreff', 'Beschreibung', 'Erstellt am', 'Erstellt von'], x) if e[1]]), axis=1)

task['insert_timestamp'] = pd.to_datetime(task['erstellt_am'])
task['owner'] = task['reg_mail']
task['description'] = task['description'].apply(lambda x: html_to_text(x))
task['title'] = task['title'].where(task['title'].notnull(), 'Email')
task['subject'] = task['subject'].where(task['subject'] != '', None)
task['description'] = task['description'].where(task['description'] != '', None)
task['faellig_am'] = task['faellig_am'].astype(str)
task['content'] = task[['title','titel','subject','description', 'faellig_am', 'created_by']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Art','Titel', 'Betreff', 'Beschreibung', 'Erstellt am', 'Erstellt von'], x) if e[1]]), axis=1)


job = job.loc[job.create_date != '0000-00-00 00:00:00']
job['insert_timestamp'] = pd.to_datetime(job['create_date'])
job['owner'] = job['reg_mail']
job['description'] = job['description'].apply(lambda x: html_to_text(x))
job['title'] = job['title'].where(job['title'].notnull(), 'Email')
job['subject'] = job['subject'].where(job['subject'] != '', None)
job['description'] = job['description'].where(job['description'] != '', None)
job['erstellt_am'] = job['erstellt_am'].astype(str)
job['content'] = job[['title','titel','subject','description', 'erstellt_am', 'created_by']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Art','Titel', 'Betreff', 'Beschreibung', 'Erstellt am', 'Erstellt von'], x) if e[1]]), axis=1)

cand = cand.loc[cand.create_date != '0000-00-00 00:00:00']
cand['insert_timestamp'] = pd.to_datetime(cand['create_date'])
cand['owner'] = cand['reg_mail']
cand['description'] = cand['description'].apply(lambda x: html_to_text(x))
cand['title'] = cand['title'].where(cand['title'].notnull(), 'Email')
cand['subject'] = cand['subject'].where(cand['subject'] != '', None)
cand['description'] = cand['description'].where(cand['description'] != '', None)
cand['erstellt_am'] = cand['erstellt_am'].astype(str)
cand['content'] = cand[['title','titel','subject','description', 'erstellt_am', 'created_by']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Art','Titel', 'Betreff', 'Beschreibung', 'Erstellt am', 'Erstellt von'], x) if e[1]]), axis=1)

# %% load to temp db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))
re1 = vincere_activity.transform_activities_temp(company, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_tasks_temp(task, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(job, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(cand, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)
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
re2.to_sql(con=engine_sqlite, name='vincere_task', if_exists='replace', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)


# re3.to_sql(con=engine_sqlite, name='vincere_activity_job', if_exists='replace', dtype=dtype, index=False)

























