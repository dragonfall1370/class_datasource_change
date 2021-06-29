# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import datetime
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ac_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
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

engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_contact
vcont = vincere_contact.Contact(engine_postgre.raw_connection())

# %% funs

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
contact = pd.read_sql("""
select
cont.CONTACT_ID as contact_externalid
, com.company_externalid
, cont.TITLE as title
, cont.FIRST_NAME as contact_firstname
, cont.LAST_NAME as contact_lastname
, cont.MIDDLE_NAME as contact_middlename
, cont.EMAIL as contact_email
, cont.CLIENT_TYPE as Client_Type
, u.EmailAddress as contact_owner
, cont.CREATED_ON as reg_date
, cont.POSITION as job_title
, cont.TELEPHONE as primary_phone
, cont.MOBILE as mobile_phone
, cont.ADDRESS as address
, cont.POST_CODE as post_code
, cont.LAST_CONTACTED
, cont.START_DATE
, cont.LEFT_DATE as END_DATE
, cont.CATEGORY_ID
, cont.MODIFY_ON as other_tab_MODIFY_ON
, u1.Name as other_tab_MODIFY_BY
, cont.CREATED_ON as other_tab_CREATED_ON
, u2.Name as other_tab_CREATED_BY
, ag.NAME as other_tab_access_group
, comaddr.Telephone as company_telephone
from Contacts cont
left join [User] u on cont.CREATED_BY = u.Id
left join AccessGroups ag on cont.GROUP_ID = ag.GROUP_ID
left join (
    select CompanyId, string_agg(Telephone, ' | ') as Telephone
    from CompanyAddress
    where Telephone is not null and Telephone != ''
    group by CompanyId
) comaddr on cont.COMPANY_ID = comaddr.CompanyId
left join [User] u1 on cont.MODIFY_BY = u1.Id
left join [User] u2 on cont.CREATED_BY = u2.Id
left join (
	select
		 c.COMPANY_ID as company_externalid
		from Companies c
		where c.DELETED = 0
) com on cont.COMPANY_ID = com.company_externalid
where cont.DELETED = 0;
;
""", engine_mssql)

contact['address'] = contact[['address', ]].apply(lambda x: ', '.join([e for e in x if e]), axis=1) \
    .map(lambda x: html_to_text(x)).map(lambda x: x.replace('\n', ', ').replace(',,', ',').replace(', ,', ','))

assert False

vcont.update_reg_date(contact, mylog)
vcont.update_job_title(contact, mylog)
vcont.update_primary_phone(contact, mylog)
vcont.update_mobile_phone(contact, mylog)
vcont.insert_current_location(contact, mylog)

df = contact
logger = mylog
tem2 = df[['contact_externalid', 'post_code']]
tem2 = tem2.merge(vcont.contact, on=['contact_externalid'])
tem2['id'] = tem2['current_location_id']
tem2.id = tem2.id.astype(int)
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcont.ddbconn, ['post_code', ], ['id', ], 'common_location', logger)


# vcont.update_current_location_post_code(contact, mylog)
vcont.set_work_location_by_company_location(mylog)

# %% note
contact = contact.merge(pd.read_sql("select * from ContactCategories;", engine_mssql),  left_on='CATEGORY_ID', right_on='CAT_ID', how='left')
contact['other_tab_access_group'] = contact['other_tab_access_group'].fillna('Unassigned')
note = contact[[
    'contact_externalid', 'Client_Type', 'CAT_NAME'
    , 'company_telephone', 'LAST_CONTACTED', 'other_tab_MODIFY_ON'
    , 'other_tab_MODIFY_BY', 'other_tab_CREATED_ON', 'other_tab_CREATED_BY'
    , 'other_tab_access_group', 'START_DATE']]
# note = note.where(note.notnull(), None)
note.other_tab_CREATED_ON = note.other_tab_CREATED_ON.dt.strftime('%d-%b-%Y %H:%M')
note.other_tab_MODIFY_ON = note.other_tab_MODIFY_ON.dt.strftime('%d-%b-%Y %H:%M')
note.LAST_CONTACTED = note.LAST_CONTACTED.astype(object).where(note.LAST_CONTACTED.notnull(), None)
note.LAST_CONTACTED = note.LAST_CONTACTED.map(lambda x: datetime.datetime.strftime(x, '%d-%b-%Y %H:%M') if x else x)
note.START_DATE = note.START_DATE.astype(object).where(note.START_DATE.notnull(), None)
note.START_DATE = note.START_DATE.map(lambda x: datetime.datetime.strftime(x, '%d-%b-%Y %H:%M') if x else x)
note['CAT_NAME'] = note['CAT_NAME'].fillna('')
note['note'] = note.apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Itris Contact ID', 'Type', 'Category'
                                            , 'Company Telephone', 'Last Contacted', 'Modified On'
                                            , 'Modified By', 'Created On', 'Created By'
                                            , 'Access Group','Start date'], x) if e[1]]), axis=1)

vcont.update_note_2(note, dest_db, mylog)


# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'title']].dropna().drop_duplicates()
tem['gender_title'] = tem['title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcont.update_gender_title(tem, mylog)

# %%
# vincere_custom_migration.execute_sql_update("update contact set insert_timestamp='1989-03-12 00:00:00' where first_name like '%DEFAULT%'", engine_postgre.raw_connection())