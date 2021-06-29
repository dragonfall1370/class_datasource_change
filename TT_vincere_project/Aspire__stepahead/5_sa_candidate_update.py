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

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
candidate = pd.read_sql("""
select concat('', csv.Person_Reference) as candidate_externalid
     , nullif(csv.Forename,'') as Forename
     , nullif(csv.Surname,'') as Surname
     , nullif(csv.Middle,'') as Middle
     , nullif(Email_Address,'') as Email_Address
     , nullif(trim(csv.Address1),'') as Address1
     , nullif(trim(csv.Address2),'') as Address2
     , nullif(trim(csv.Address3),'') as Address3
     , nullif(trim(csv.Address4),'') as Address4
     , nullif(trim(csv.Address5),'') as Address5
     , nullif(trim(csv.Town),'') as Town
     , nullif(trim(csv.PostCode),'') as Post_Code
     , nullif(trim(csv.County),'') as County
     , nullif(trim(Home_Telephone),'') as Home_Telephone
     , nullif(trim(Work_Telephone),'') as Work_Telephone
     , nullif(trim(Mobile),'') as Mobile
     , PermConsultantReference
     , TempConsultantReference
     , WorkTypes
from Candidate_Search_View csv
join Candidate_Merge_View cmv on csv.Person_Reference = cmv.Reference
""", engine_mssql)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','Address1', 'Address2', 'Address3', 'Address4', 'Address5', 'Town', 'Post_Code', 'County']]
c_location['address'] = c_location[['Address1', 'Address2', 'Address3', 'Address4', 'Address5', 'Town', 'Post_Code', 'County']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['address']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid', 'Address1', 'Address2', 'Address3', 'Address4', 'Address5', 'Town', 'Post_Code', 'County']].drop_duplicates()\
    .rename(columns={'Town': 'city', 'County': 'state', 'Post_Code': 'post_code'})
tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
tem = comaddr[['candidate_externalid']]
tem['country_code'] = 'GB'
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code2(tem, dest_db, mylog)

# %% home phones
home_phone = candidate[['candidate_externalid', 'Home_Telephone']].drop_duplicates().dropna()
home_phone['home_phone'] = home_phone['Home_Telephone']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'Work_Telephone']].drop_duplicates().dropna()
wphone['work_phone'] = candidate['Work_Telephone']
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile
indt = candidate[['candidate_externalid', 'Mobile']].dropna().drop_duplicates()
indt['mobile_phone'] = indt['Mobile']
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)

# %% primary phones
indt = candidate[['candidate_externalid', 'Mobile']].dropna().drop_duplicates()
indt['primary_phone'] = indt['Mobile']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)

# %% job type
tem1 = candidate[['candidate_externalid', 'PermConsultantReference']].dropna().drop_duplicates()
tem1 = tem1.loc[tem1['PermConsultantReference'] != -1]
tem1['desired_job_type'] = 'permanent'
tem1.drop(['PermConsultantReference'], axis=1, inplace=True)
tem2 = candidate[['candidate_externalid', 'TempConsultantReference']].dropna().drop_duplicates()
tem2 = tem2.loc[tem2['TempConsultantReference'] != -1]
tem2['desired_job_type'] = 'contract'
tem2.drop(['TempConsultantReference'], axis=1, inplace=True)
tem=pd.concat([tem1,tem2])
cp = vcand.update_desired_job_type(tem, mylog)

# %% gender
tem = pd.read_sql("""
select concat('', Reference) as candidate_externalid
     , Gender
from Candidate_Report_1
""", engine_mssql)
tem = tem.drop_duplicates()
tem['Gender'].unique()
tem.loc[tem['Gender']=='Male', 'male'] = 1
tem.loc[tem['Gender']=='Female', 'male'] = 0
tem2 = tem[['candidate_externalid','male']].dropna()
tem2['male'] = tem2['male'].astype(int)
cp = vcand.update_gender(tem2, mylog)

# %% info
candidate_info = pd.read_sql("""
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
select concat('', Reference) as candidate_externalid
     , nullif(trim(Detail),'') as Detail
     , nullif(trim(Value),'') as Value
     , nullif(trim(FactoidGroup),'') as FactoidGroup
     , System
     , Reference_Type
from Further_detail f
join Candidate_Search_View csv on csv.Person_Reference = f.Reference
where 1=1""", engine_mssql)

# %% emergency_name
tem1 = candidate_info.query('Detail == "Contact Name"')
tem2 = candidate_info.query('Detail == "Name 1"')
tem3 = candidate_info.query('Detail == "Name 2"')
tem1 = tem1.drop_duplicates()
tem2 = tem2.drop_duplicates()
tem3 = tem3.drop_duplicates()
tem1['emergency_name'] = tem1['Value']
tem2['emergency_name'] = tem2['Value']
tem3['emergency_name_z'] = tem3['Value']
tem = tem1[['candidate_externalid','emergency_name']].merge(tem2[['candidate_externalid','emergency_name']], on='candidate_externalid',how='left')
tem = tem.merge(tem3[['candidate_externalid','emergency_name_z']], on='candidate_externalid',how='left')
tem = tem.where(tem.notnull(),None)
tem['emergency_name'] = tem[['emergency_name_x','emergency_name_y','emergency_name_z']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp = vcand.emergency_name(tem, mylog)

# %% emergency_relationship
tem1 = candidate_info.query('Detail == "Contact Relationship"')
tem2 = candidate_info.query('Detail == "Relationship 1"')
tem3 = candidate_info.query('Detail == "Relationship 2"')
tem1 = tem1.drop_duplicates()
tem2 = tem2.drop_duplicates()
tem3 = tem3.drop_duplicates()
tem1['emergency_relationship'] = tem1['Value']
tem2['emergency_relationship'] = tem2['Value']
tem3['emergency_relationship_z'] = tem3['Value']
tem = tem1[['candidate_externalid','emergency_relationship']].merge(tem2[['candidate_externalid','emergency_relationship']], on='candidate_externalid',how='left')
tem = tem.merge(tem3[['candidate_externalid','emergency_relationship_z']], on='candidate_externalid',how='left')
tem = tem.where(tem.notnull(),None)
tem['emergency_relationship'] = tem[['emergency_relationship_x','emergency_relationship_y','emergency_relationship_z']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp = vcand.emergency_relationship(tem, mylog)

# %% emergency_phone
tem1 = candidate_info.query('Detail == "Contact Tel Day"')
tem2 = candidate_info.query('Detail == "Contact 1 Tel"')
tem3 = candidate_info.query('Detail == "Contact 2 Tel"')
tem1 = tem1.drop_duplicates()
tem2 = tem2.drop_duplicates()
tem3 = tem3.drop_duplicates()
tem1['emergency_phone'] = tem1['Value']
tem2['emergency_phone'] = tem2['Value']
tem3['emergency_phone_z'] = tem3['Value']
tem = tem1[['candidate_externalid','emergency_phone']].merge(tem2[['candidate_externalid','emergency_phone']], on='candidate_externalid',how='left')
# tem = tem.merge(tem3[['candidate_externalid','emergency_phone_z']], on='candidate_externalid',how='left')
tem = tem.where(tem.notnull(),None)
tem['emergency_phone'] = tem[['emergency_phone_x','emergency_phone_y']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem['emergency_phone'] = tem['emergency_phone'].apply(lambda x: x[:50])
cp = vcand.emergency_phone(tem, mylog)

# %% dob
tem = candidate_info.query('Detail == "Date of Birth"')
tem = tem.drop_duplicates()
tem = tem.loc[tem['Value']!='03/01/76SKYPE: abulayla11']
tem['date_of_birth'] = pd.to_datetime(tem['Value'])
vcand.update_dob(tem, mylog)

# %% visa note
tem = candidate_info.query('Detail == "Right to Work Checked"')
tem = tem.drop_duplicates()
tem['visa_note'] = tem['Value']
tem1 = tem[['candidate_externalid','visa_note']].dropna()
tem1.loc[tem1['visa_note']=='True', 'vnote'] = 'Yes'
tem1.loc[tem1['visa_note']=='False', 'vnote'] = 'No'
tem2 = tem1[['candidate_externalid','vnote']].dropna()
tem2['visa_note'] = 'Right to Work Checked: '+ tem2['vnote']
cp = vcand.update_visa_note(tem2, dest_db, mylog)

# %% current salary
tem = candidate_info.query('Detail == "Current Salary"')
tem = tem.drop_duplicates()
tem['current_salary'] = tem['Value']
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% desired salary
tem = candidate_info.query('Detail == "Ideal Salary"')
tem = tem.drop_duplicates()
tem['desire_salary'] = tem['Value']
tem['desire_salary'] = tem['desire_salary'].astype(float)
vcand.update_desire_salary(tem, mylog)

# %% notice
tem = candidate_info.query('Detail == "Notice Weeks"')
tem = tem.drop_duplicates()
tem['Value'].unique()
tem.loc[tem['Value']=='1.00', 'notice_period'] = 7
tem.loc[tem['Value']=='0.00', 'notice_period'] = 0
tem.loc[tem['Value']=='4.00', 'notice_period'] = 28
tem.loc[tem['Value']=='2.00', 'notice_period'] = 14
tem.loc[tem['Value']=='9.00', 'notice_period'] = 63
tem2 = tem[['candidate_externalid','notice_period']].dropna()
vcand.update_notice_period(tem2, mylog)

# %% contract rate
tem = candidate_info.query('Detail == "Min Rate"')
tem = tem.drop_duplicates()
tem['contract_rate'] = tem['Value']
tem['contract_rate'] = tem['contract_rate'].astype(float)
vcand.update_contract_rate(tem, mylog)


# %% note
note = candidate[['candidate_externalid']]
note1 = pd.read_sql("""
select concat('', Reference) as candidate_externalid
     , nullif(trim(Company_Name),'') as Company_Name
     , nullif(trim(Referee_Title),'') as Referee_Title
     , nullif(trim(Referee_Forename),'') as Referee_Forename
     , nullif(trim(Referee_Surname),'') as Referee_Surname
     , nullif(trim(Referee_KnownAs),'') as Referee_KnownAs
     , nullif(trim(Job_Title),'') as Job_Title
     , nullif(trim(Referee_Address),'') as Referee_Address
     , nullif(trim(Referee_Main_Phone),'') as Referee_Main_Phone
     , nullif(trim(Referee_Mobile_Phone),'') as Referee_Mobile_Phone
     , nullif(trim(Referee_Email_Address),'') as Referee_Email_Address
     , nullif(convert(nvarchar(max),Referee_Notes),'') as Referee_Notes
from Candidate_Referee_View
""", engine_mssql)

note2 = pd.read_sql("""
select concat('', Person_Reference) as candidate_externalid
     , nullif(trim(Candidate_Reference),'') as Candidate_Reference 
     , NI_Number
     , Previous_Gross
     , Previous_Tax
     , Sort_Code
     , Account_Number
     , Account_Name
     , Building_Society_Number
     , Person_Reference, NI_Number, NI_Letter, Tax_Code
from DB_Candidate_Payroll_Details
""", engine_mssql)

tem1 = candidate_info[['candidate_externalid','Value','Detail']].query('Detail == "Benefits Detail"').rename(columns={'Value':'Benefits Detail'})
tem1 = tem1.groupby('candidate_externalid')['Benefits Detail'].apply(lambda x: '\n'.join(x)).reset_index()
tem1 = tem1.drop_duplicates()

tem2 = candidate_info[['candidate_externalid','Value','Detail']].query('Detail == "Benefits Recipient"').rename(columns={'Value':'Benefits Recipient'})
tem2 = tem2.groupby('candidate_externalid')['Benefits Recipient'].apply(lambda x: '\n'.join(x)).reset_index()
tem2 = tem2.drop_duplicates()

tem3 = candidate_info[['candidate_externalid','Value','Detail']].query('Detail == "Is a care-leaver?"').rename(columns={'Value':'Is a care-leaver?'})
tem3 = tem3.groupby('candidate_externalid')['Is a care-leaver?'].apply(lambda x: '\n'.join(x)).reset_index()
tem3 = tem3.drop_duplicates()

tem4 = candidate_info[['candidate_externalid','Value','Detail']].query('Detail == "Is a carer?"').rename(columns={'Value':'Is a carer?'})
tem4 = tem4.groupby('candidate_externalid')['Is a carer?'].apply(lambda x: '\n'.join(x)).reset_index()
tem4 = tem4.drop_duplicates()

tem5 = candidate_info[['candidate_externalid','Value','Detail']].query('Detail == "Is Unemployed?"').rename(columns={'Value':'Is Unemployed?'})
tem5 = tem5.groupby('candidate_externalid')['Is Unemployed?'].apply(lambda x: '\n'.join(x)).reset_index()
tem5 = tem5.drop_duplicates()

tem6 = candidate_info[['candidate_externalid','Value','Detail']].query('Detail == "Nationality"').rename(columns={'Value':'Nationality'})
tem6 = tem6.groupby('candidate_externalid')['Nationality'].apply(lambda x: '\n'.join(x)).reset_index()
tem6 = tem6.drop_duplicates()

note6 = pd.read_sql("""
Select nullif(convert(nvarchar(max),an.Notes),'') as Notes, concat('', dl.Reference) as candidate_externalid
from Notes an
inner join Details_Lookup dl
on dl.Detail_reference = an.Reference
and an.notes_name = 'Candidate_Notes'
where dl.Detail_Type = 7
""", engine_mssql)
note6 = note6.dropna().drop_duplicates()
# note6.loc[note6['company_externalid']=='2623']
note6 = note6.groupby('candidate_externalid')['Notes'].apply(lambda x: '\n--------------------\n '.join(x)).reset_index()

# tem7 = candidate_info[['candidate_externalid','Value']].query('Detail == "Benefits Detail"').rename(columns={'Value':'Benefits Detail'})
# tem7 = tem7.groupby('candidate_externalid')['Benefits Detail'].apply(lambda x: '\n'.join(x)).reset_index()
# tem7 = tem7.drop_duplicates()

note = note.merge(note1, on='candidate_externalid', how='left')
note = note.merge(note2, on='candidate_externalid', how='left')
note = note.merge(tem1, on='candidate_externalid', how='left')
note = note.merge(tem2, on='candidate_externalid', how='left')
note = note.merge(tem3, on='candidate_externalid', how='left')
note = note.merge(tem4, on='candidate_externalid', how='left')
note = note.merge(tem5, on='candidate_externalid', how='left')
note = note.merge(tem6, on='candidate_externalid', how='left')
note = note.merge(note6, on='candidate_externalid', how='left')
note = note.fillna('')
note['Payroll Number'] = 'No Data Row Found'
note['Status'] = 'No Data Row Found'
note['Gross Year To Date'] = 'No Data Row Found'

note['note'] = note[['Previous_Gross', 'Previous_Tax','Notes','Benefits Detail','Benefits Recipient','Is a care-leaver?','Is a carer?','Is Unemployed?','Nationality'
    ,'Payroll Number', 'Company_Name','Referee_Title','Referee_Forename','Referee_Surname','Referee_KnownAs','Job_Title','Referee_Address','Referee_Main_Phone','Referee_Mobile_Phone','Referee_Email_Address','Referee_Notes','Status'
    ,'Candidate_Reference','Tax_Code','NI_Number','NI_Letter','Gross Year To Date']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Previous Gross', 'Previous Tax','Notes','Benefits Detail','Benefits Recipient','Is a care-leaver?','Is a carer?','Is Unemployed?','Nationality'
    ,'Payroll Number', 'Referee Company Name','Referee Title','Referee Forename','Referee Surname','Referee KnownAs','Referee Job Title','Referee Address','Referee Main Phone','Referee Mobile Phone','Referee Email Address','Referee Notes','Referee Status'
    ,'Employee Reference','Tax Code','NI Number','NI Code','Gross Year To Date'], x) if e[1]]), axis=1)
cp7 = vcand.update_note2(note, dest_db, mylog)

# %% marital
tem = candidate_info.query('Detail == "Marital Status"')
tem.loc[tem['marital']=='Single', 'maritalstatus'] = 1
tem.loc[tem['marital']=='Married', 'maritalstatus'] = 2
tem.loc[tem['marital']=='Widowed', 'maritalstatus'] = 4
tem2 = tem[['candidate_externalid','maritalstatus']].dropna().drop_duplicates()
tem2['maritalstatus'] = tem2['maritalstatus'].apply(lambda x: int(str(x).split('.')[0]))
vcand.update_marital_status(tem2, mylog)

# %% source
tem = candidate_info.query('Detail == "Source"')
tem = tem.drop_duplicates()
tem['source'] = tem['Value']
cp = vcand.insert_source(tem)

# %% current employer
cur_emp = pd.read_sql("""with wh as (
select concat('', Reference) as candidate_externalid
     , title
     , company_name
, ROW_NUMBER() OVER(PARTITION BY Reference ORDER BY Ending_Date DESC) rn
from Work_History_View)
select * from wh where rn =1""", engine_mssql)

cur_emp['current_employer'] = cur_emp['company_name'].str.strip()
cur_emp['current_job_title'] = cur_emp['title'].str.strip()
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = candidate[['candidate_externalid', 'title']].dropna().drop_duplicates()
# tem['gender_title'] = tem['title']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# vcand.update_gender_title(tem, mylog)

# %% reg date
tem = pd.read_sql("""
select concat('', c.Person_Reference) as candidate_externalid
,Diary_Date
from
DB_Candidate_Diary_Details c
where Event_Description = 'Candidate registered                              '
""", engine_mssql)
tem = tem.drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['Diary_Date'])
vcand.update_reg_date(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# %% last activity date
tem = pd.read_sql("""
select concat('', Person_Reference) as candidate_externalid
     , max(Created) as Created
from DB_Candidate_Diary_Details
group by Person_Reference
""", engine_mssql)
tem = tem.drop_duplicates()
tem['last_activity_date'] = pd.to_datetime(tem['Created'])
vcand.update_last_activity_date(tem, mylog)

# %% distribution list
d_list = pd.read_sql("""
select concat('', si.Reference) as candidate_externalid
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
where Reference_Type = 3
and Description is not null
""", engine_mssql)
tem = d_list[['Description','owner','Share_Type']].drop_duplicates()
tem['name'] = tem['Description']
tem['Share_Type'].unique()
tem.loc[tem['Share_Type']=='Global', 'share_permission'] = 1
tem.loc[tem['Share_Type']=='Private', 'share_permission'] = 2
tem.loc[tem['Share_Type']=='Planner', 'share_permission'] = 2
tem = tem.fillna('')
vcand.create_talent_pool(tem,mylog)

tem1 = d_list[['Description','candidate_externalid']].drop_duplicates()
tem1['group_name'] = tem1['Description']
vcand.add_candidate_talent_pool(tem1,mylog)