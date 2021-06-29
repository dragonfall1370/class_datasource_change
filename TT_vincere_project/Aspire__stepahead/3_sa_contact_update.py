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
cf.read('sa_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# %% info
contact = pd.read_sql("""
select distinct
nullif(Forename,'') as firstname
, nullif(Surname,'') as lastname
, nullif(middle,'') as middlename
, nullif(lower(trim(Email)),'') as email, nullif(trim(Mobile),'') as Mobile, nullif(trim(Phone),'') as Phone, nullif(trim(Liaison_Type),'') as Job_Title
, com.Reference
, concat(ccv.Reference,'_',com.Reference) as contact_externalid
from Company_Contact_View ccv
join (select Reference, Company_Name from (select Company_Name, Company.Reference, Client.Reference as client_ref from Company
    join Client on Client.Company_Reference = Company.Reference) c
join(
select distinct Company_Reference
From Diary_Entry a
Inner join diary_entry_blueprint d
on a.Blueprint_Reference = d.Reference
Inner join type_description e
on d.Diary_type = e.reference
Inner join Diary_Entry_Lookup f
on a.Reference = f.Reference
and f.Entity_Type = 4
Inner join DB_Client_Basic_Details g
on f.Entity_Reference = g.Company_Reference
Where consultant_reference is not null
and e.type = 'Diary'
and Created between '2016-10-01 00:00:00' and '2020-10-01 00:00:00'
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference) com on ccv.liaison_Reference = com.Reference
""", engine_mssql)

assert False
# # %% location name/address
vcont.set_work_location_by_company_location(mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'Job_Title']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['Job_Title']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'Phone']].dropna().drop_duplicates()
tem['primary_phone'] = tem['Phone']
vcont.update_primary_phone(tem, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'Mobile']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['Mobile']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
# tem = contact[['contact_externalid', 'PersonHomeTelephone']].dropna().drop_duplicates()
# tem['home_phone'] = tem['PersonHomeTelephone']
# vcont.update_home_phone(tem, mylog)

# %% personal email
# tem = contact[['contact_externalid', 'PersonHomeEMail']].dropna().drop_duplicates()
# tem['personal_email'] = tem['PersonHomeEMail']
# vcont.update_personal_email(tem, mylog)

# %% primary email
# email = contact[['contact_externalid', 'EmailWork']].dropna().drop_duplicates()
# email['email'] = email[['EmailWork']]
# vcont.update_email(email, mylog)

# %% reg date
# tem = contact[['contact_externalid', 'PersonCreateDate']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['PersonCreateDate'])
# vcont.update_reg_date(tem, mylog)

# %% last activity date
tem = pd.read_sql("""
with cont as (
select concat('', Contact_Person_Reference) as contact_externalid_1, max(Created) as Created
     , com.Reference
--      , row_number() over (partition by com.Reference order by Contact_Person_Reference) as rn
from DB_Client_Contact_Diary_Details cd
join (select Reference, Company_Name, client_ref from (select Company_Name, Company.Reference, Client.Reference as client_ref from Company
    join Client on Client.Company_Reference = Company.Reference) c
join(
select distinct Company_Reference
From Diary_Entry a
Inner join diary_entry_blueprint d
on a.Blueprint_Reference = d.Reference
Inner join type_description e
on d.Diary_type = e.reference
Inner join Diary_Entry_Lookup f
on a.Reference = f.Reference
and f.Entity_Type = 4
Inner join DB_Client_Basic_Details g
on f.Entity_Reference = g.Company_Reference
Where consultant_reference is not null
and e.type = 'Diary'
and Created between '2016-10-01 00:00:00' and '2020-10-01 00:00:00'
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference) com on cd.Company_Reference = com.client_ref
group by Contact_Person_Reference, com.Reference)
select *, concat(contact_externalid_1,'_',Reference) as contact_externalid from cont;
""", engine_mssql)
tem = tem.drop_duplicates()
tem['last_activity_date'] = pd.to_datetime(tem['Created'])
vcont.update_last_activity_date(tem, mylog)

# %% note
external_id = pd.read_sql("""
select external_id from contact where deleted_timestamp is null and external_id is not null and external_id  not like '%DEFAULT%'
""", connection)
external_id['contact_externalid'] = external_id['external_id'].apply(lambda x: x.split('_')[0])

note = pd.read_sql("""
Select nullif(convert(nvarchar(max),an.Notes),'') as note, concat('', dl.Reference) as contact_externalid
from Notes an
inner join Details_Lookup dl
on dl.Detail_reference = an.Reference
and an.notes_name = 'Liaison_Notes'
where dl.Detail_Type = 7
""", engine_mssql)
note['contact_externalid'] = note['contact_externalid'].apply(lambda x: x.strip())

tem = note.merge(external_id, on='contact_externalid')
tem2 = tem[['external_id','note']]
tem2 = tem2.dropna().drop_duplicates()
tem2['contact_externalid'] = tem2['external_id']
# note = note.groupby('contact_externalid')['note'].apply(lambda x: '\n--------------------\n '.join(x)).reset_index()
cp7 = vcont.update_note_2(tem2, dest_db, mylog)

# %% mail_sub
detail = pd.read_sql("""
with Further_detail AS (
Select b.Reference, a.Reference as Detail_Reference, Detail, Value, 'S' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from string_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 1
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'N' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Number_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 2
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, Convert(Char(10), Value, 103) as Value, 'D' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Date_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 3
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'M' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Money_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 4
-- and System = '0'

union

Select b.Reference, a.Reference as Detail_Reference, Detail, Case Value when 1 then 'True' else 'False' end as Value, 'B' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Boolean_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 5
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'F' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Float_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 8
-- and System = '0'
)
select concat('', Reference) as contact_externalid
, nullif(trim(Detail),'') as Detail
, nullif(trim(Value),'') as Value
, nullif(trim(FactoidGroup),'') as FactoidGroup, System, Reference_Type
from Further_detail
where Detail = 'Newsletter'
-- and System = '0'
""", engine_mssql)
tem = contact[['contact_externalid', 'email']].dropna().drop_duplicates()
tem['contact_externalid'] = tem['contact_externalid'].apply(lambda x: x.split('_')[0])
tem = tem.drop_duplicates()
tem = tem.merge(detail,on='contact_externalid')

tem1 = tem[['email','Value']]
tem1 = tem1.drop_duplicates()
tem1['Value'].unique()
tem1.loc[tem1['Value']=='True', 'subscribed'] = 1
tem1.loc[tem1['Value']=='No', 'subscribed'] = 0
tem1.loc[tem1['Value']=='False', 'subscribed'] = 0
tem2 = tem1[['email', 'subscribed']].dropna().drop_duplicates()
tem2['rn'] = tem2.groupby('email').cumcount()
tem2.loc[tem2['email']=='yvonne.sanders@walthamforest.gov.uk']
tem3 = tem2.loc[tem2['rn']==0]
tem4 = tem2.loc[tem2['rn']!=0]
tem3 = tem3.loc[~tem3['email'].isin(tem4['email'])]
tem5 = pd.concat([tem3,tem4])
tem5 = tem5.drop_duplicates()
tem5['rn'] = tem5.groupby('email').cumcount()
tem5.loc[tem5['rn']>0]
cp7 = vcont.email_subscribe(tem5, mylog)

# %%
cont = pd.read_sql("""select id, company_id from contact where deleted_timestamp is null""", engine_postgre_review)
comp = pd.read_sql("""select id as company_id, insert_timestamp from company where deleted_timestamp is null""", engine_postgre_review)
tem = cont.merge(comp, on='company_id')
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcont.ddbconn, ['insert_timestamp'], ['id'], 'contact', mylog)

# %% distribution list
d_list = pd.read_sql("""
select concat('', si.Reference) as contact_externalid
     , nullif(trim(Description),'') as Description
     , nullif(trim(Email),'') as owner
     , CASE WHEN Share_Type=1 THEN 'Private' WHEN Share_Type=2 THEN 'Planner' ELSE 'Global' END AS Share_Type
from Savelist_Item si
left join Savelist_Header sh on sh.Reference = si.Header_Reference
left join (
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '') AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) o1 on o1.Reference = Consultant_Reference
where Reference_Type = 5
and Description is not null
""", engine_mssql)
tem = d_list[['Description','owner','Share_Type']].drop_duplicates()
tem['name'] = tem['Description']
tem.loc[tem['Share_Type']=='Global', 'share_permission'] = 1
tem.loc[tem['Share_Type']=='Private', 'share_permission'] = 2
tem = tem.fillna('')
vcont.create_distribution_list(tem,mylog)

tem1 = d_list[['Description','contact_externalid']].drop_duplicates()
tem1['group_name'] = tem1['Description']
vcont.add_contact_distribution_list(tem1,mylog)