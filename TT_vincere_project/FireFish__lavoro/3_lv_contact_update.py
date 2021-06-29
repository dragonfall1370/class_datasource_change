# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
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
cf.read('lv_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
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
connection = engine_postgre.raw_connection()

from common import vincere_contact
vcont = vincere_contact.Contact(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)
# from common import parse_gender_title

#%% mailing address
contact = pd.read_sql("""
select p.ID as contact_externalid
     , p.FirstName
     , p.MiddleName
     , p.Surname
     , p.KnownAs
     , p.WorkMob
     , p.DirectDial
     , p.WorkEMail
     , p.JobTitle
     , o.WorkEMail as owner
     , com.CompanyID
     , com.DateFrom,p.DisplayID
     , PhoneMobile, p.ContactKeywords, t.Value as title, p.ContactLastActionDate
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select * from CandidateEmployment where DateTo is null) com on p.ID = com.CandidateID
left join Drop_Down___Titles t on p.TitleWSIID = t.ID
where c.IsActivated = 0
and p.IsActivated = 1
and p.IsArchived = 0
UNION
select p.ID as contact_externalid
     , p.FirstName
     , p.MiddleName
     , p.Surname
     , p.KnownAs
     , p.WorkMob
     , p.DirectDial
     , p.WorkEMail
     , p.JobTitle
     , o.WorkEMail as owner
     , com.CompanyID
     , com.DateFrom,p.DisplayID
     , PhoneMobile, p.ContactKeywords, t.Value as title, p.ContactLastActionDate
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select * from CandidateEmployment where DateTo is null) com on p.ID = com.CandidateID
left join Drop_Down___Titles t on p.TitleWSIID = t.ID
where c.IsActivated = 1
and p.IsActivated = 1
and p.IsArchived = 0
""", engine_sqlite)
contact.sort_values('DateFrom', inplace=True, ascending=False)
contact['rn'] = contact.groupby('contact_externalid').cumcount()
contact = contact.loc[contact['rn'] == 0]
contact['contact_externalid'] = contact['contact_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'title']].dropna().drop_duplicates()
tem['gender_title'] = tem['title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
cp = vcont.update_gender_title(tem, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'JobTitle']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['JobTitle']
vcont.update_job_title(jobtitle, mylog)

# %% preferred name
# tem = contact[['contact_externalid', 'KnownAs']].dropna().drop_duplicates()
# tem['preferred_name'] = tem['KnownAs']
# vcont.update_preferred_name(tem, mylog)

# %% primary phone
primary_phone = contact[['contact_externalid', 'DirectDial']].dropna().drop_duplicates()
primary_phone['primary_phone'] = primary_phone['DirectDial']
vcont.update_primary_phone(primary_phone, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'WorkMob']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['WorkMob']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% skill
# tem = contact[['contact_externalid', 'ContactKeywords']].dropna().drop_duplicates()
# tem['skills'] = tem['ContactKeywords']
# vcont.update_skills(tem, mylog)

# %% last activity date
tem = contact[['contact_externalid', 'ContactLastActionDate']].dropna()
tem['last_activity_date'] = pd.to_datetime(tem['ContactLastActionDate'])
vcont.update_last_activity_date(tem, mylog)

# %% note
legal = pd.read_sql("""
select CandidateID as contact_externalid, FriendlyName||' '||Date as legal_item from LegalItemCandidateHistory l left join LegalType li on l.LegalItemID = li.ID
""", engine_sqlite)
legal['contact_externalid'] = legal['contact_externalid'].apply(lambda x: str(x) if x else x)
note_cont = pd.read_sql("""
select ad.ContactID as contact_externalid
     , an.Notes as notes
from ActionDetail ad
left join ActionNote an on ad.ActionNoteID = an.ID
left join Person p on p.ID = ad.PersonID
left join ActionType at on ActionID = at.ID
-- where an.notes like '%Happy to meet candidates early%'
where at.DisplayName = 'Comments' and ContactID is not null
""", engine_sqlite)
note_cont['contact_externalid'] = note_cont['contact_externalid'].apply(lambda x: str(x) if x else x)
note_cont = note_cont.groupby('contact_externalid')['notes'].apply('\n\n'.join).reset_index()

note = contact[['contact_externalid','DisplayID']]
note = note.merge(legal, on ='contact_externalid', how='left')
note = note.merge(note_cont, on ='contact_externalid', how='left')
note = note.where(note.notnull(), None)
note['note'] = note[['DisplayID', 'legal_item','notes']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Lavoro ID', 'Legal', 'Notes'], x) if e[1]]), axis=1)
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% reg date
# reg_date = contact[['contact_externalid', 'CreatedDate']].dropna().drop_duplicates()
# reg_date['CreatedDate'] = pd.to_datetime(reg_date['CreatedDate'])
# reg_date['CreatedDate'] = reg_date['CreatedDate'].apply(lambda x: datetime.datetime.strftime(x, '%m/%d/%Y %H:%M'))
# reg_date['reg_date'] = pd.to_datetime(reg_date['CreatedDate'])
# vcont.update_reg_date(reg_date, mylog)

# %% social
social = pd.read_sql("""
select p.ID as contact_externalid, detail.*
from Person p
left join (select cd.Detail, cd.PersonID, cdt.Name as type
from ContactDetail cd
left join ContactDetailType cdt on cdt.ID = TypeID) detail on p.ID = detail.PersonID
""", engine_sqlite)
social['contact_externalid'] = social['contact_externalid'].apply(lambda x: str(x) if x else x)
social = social.dropna()
social['Detail'] = social['Detail'].apply(lambda x: x[:100])
social['type'].unique()

lk = social.loc[social['type'] == 'LinkedIn']
lk['linkedin'] = lk['Detail']
skype = social.loc[social['type'] == 'Skype']
skype['skype'] = skype['Detail']
tw = social.loc[social['type'] == 'Twitter']
tw['twitter'] = tw['Detail']
fb = social.loc[social['type'] == 'Facebook']
fb['facebook'] = fb['Detail']

vcont.update_linkedin(lk, mylog)
vcont.update_skype(skype, mylog)
vcont.update_facebook(fb, mylog)
vcont.update_twitter(tw, mylog)

# %% mail_sub
tem = contact[['WorkEMail']].dropna().drop_duplicates()
tem['email'] = tem['WorkEMail']
tem['subscribed'] = 1
cp7 = vcont.email_subscribe(tem, mylog)

