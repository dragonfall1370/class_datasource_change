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
# %%
sql ="""select concat('', LookupRef) as company_externalid
     , caw.company_name
     , nullif(trim(Address1),'') as Address1
     , nullif(trim(Address2),'') as Address2
     , nullif(trim(Address3),'') as Address3
     , nullif(trim(Address4),'') as Address4
     , nullif(trim(Address5),'') as Address5
     , nullif(trim(caw.Town),'') as Town
     , nullif(trim(Post_Code),'') as Post_Code
     , nullif(trim(caw.County),'') as County
     , nullif(trim(Work_Phone),'') as Work_Phone
--      , concat_ws(' ',trim(Address1),trim(Address2),trim(Address3),trim(Address4),trim(Address5),trim(caw.Town),trim(caw.County),trim(Post_Code)) as addr
--      , nullif(Address_Lines,'') as Address_Lines
--      , nullif(csv.Town,'') as Town_2
--      , nullif(csv.County,'') as County_2
--      , nullif(PostCode,'') as PostCode
     , Client_Type
from Client_Address_View_1 caw
left join Client_Search_View csv on csv.Company_Reference = caw.LookupRef"""
company = pd.read_sql(sql, engine_mssql)
assert False
c_prod = pd.read_sql("""select id, name from company where deleted_timestamp is null""", engine_postgre_review)
c_prod['name'] = c_prod['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(c_prod, vcom.ddbconn, ['name'], ['id'], 'company', mylog)
# %% HQ address
hq_address = company[['company_externalid','Address1', 'Address2', 'Address3', 'Address4', 'Address5','Town','Post_Code','County','Client_Type']]
hq_address['address'] = hq_address[['Address1', 'Address2', 'Address3', 'Address4', 'Address5','Town','Post_Code','County']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
hq_address['location_name'] = hq_address['address']
hq_address = hq_address.loc[hq_address['address']!='']
cp2 = vcom.insert_company_location_2(hq_address, dest_db, mylog)
# %% city
hq_address['city'] = hq_address['Town']
cp3 = vcom.update_location_city_2(hq_address, dest_db, mylog)

# %% postcode
hq_address['post_code'] = hq_address['Post_Code']
cp4 = vcom.update_location_post_code_2(hq_address, dest_db, mylog)

# %% state
hq_address['state'] = hq_address['County']
cp5 = vcom.update_location_state_2(hq_address, dest_db, mylog)

# %% country
# hq_address['country_code'] = hq_address.ClientCountry.map(vcom.get_country_code)
hq_address['country_code'] = 'GB'
hq_address['country'] = 'United Kingdom'
cp6 = vcom.update_location_country_2(hq_address, dest_db, mylog)

# %% location type
hq_address['Client_Type'].unique()
hq_address.loc[hq_address['Client_Type']=='Head Office', 'location_type'] = 'HEADQUARTER'
tem = hq_address[['company_externalid','address','location_type']].dropna()
cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'Work_Phone']].dropna()
tem['switch_board'] = tem['Work_Phone']
tem['phone'] = tem['Work_Phone']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %% owner
tem = pd.read_sql("""
select concat('', Company_Reference) as company_externalid
     , nullif(trim(cv.Email),'') as email

from Client_Search_View c
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
                        ON a.person_reference = b.reference) cv on cv.Reference = c.Company_Consultant
""", engine_mssql)
tem = tem.dropna()
vcom.update_owner(tem, mylog)

# %% reg date
# tem = pd.read_sql("""
# select concat('', Company_Reference) as company_externalid
#      , Diary_Date
#      , Event_Description
# from DB_Client_Diary_Details
# where Event_Description = 'Client registered                                 '
# """, engine_mssql)
tem = pd.read_sql("""
select concat('', Company_Reference) as company_externalid
     , min(DateAndTime) as DateAndTime
from DB_Client_Diary_Details
group by Company_Reference
""", engine_mssql)
tem['reg_date'] = pd.to_datetime(tem['DateAndTime'])
vcom.update_reg_date(tem, mylog)

# %% web
tem = pd.read_sql("""
select concat('', cl.reference) as company_externalid
     ,com.company_name
     ,nullif(trim(co.information),'') as website
     ,td.Description
from dbo.contact_lookup cl
left join dbo.type_description td on td.reference=cl.contact_type and td.type='Contact'
left join dbo.contact co on co.reference=cl.contact_reference
left join dbo.company com on com.reference=cl.reference
where 1=1
and Description = 'Web Address'
""", engine_mssql)
tem = tem.drop_duplicates()
vcom.update_website(tem, mylog)

# %% last activity date
tem = pd.read_sql("""
select concat('', Company_Reference) as company_externalid
     , max(Created) as Created
from DB_Client_Diary_Details
group by Company_Reference
""", engine_mssql)
tem = tem.drop_duplicates()
tem['last_activity_date'] = pd.to_datetime(tem['Created'])
vcom.update_last_activity_date(tem, mylog)

# %% note
company = pd.read_sql("""
select
concat('', Reference) as company_externalid
, Company_Name as company_name
, getdate() as created_date
from Company c;
""", engine_mssql)

note = pd.read_sql("""
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
select concat('', Reference) as company_externalid
, nullif(trim(Detail),'') as Detail
, nullif(trim(Value),'') as Value
, nullif(trim(FactoidGroup),'') as FactoidGroup, System, Reference_Type
from Further_detail
-- where Reference_Type = 2
-- and System = '0'
""", engine_mssql)
note1 = note.query('System == False and Reference_Type == 2')
note1 = note1.drop_duplicates()
note1['Detail'].unique() #TOB Signed, Year End, No Temps, 'Hardest Posn', 'Rec Methods Used','Referred by

tem1 = note1[['company_externalid','Value']].loc[note1['Detail']=='TOB Signed'].rename(columns={'Value':'TOB Signed'})

tem1 = tem1.groupby('company_externalid')['TOB Signed'].apply(lambda x: '\n'.join(x)).reset_index()
tem1 = tem1.drop_duplicates()

tem2 = note1[['company_externalid','Value']].loc[note1['Detail']=='Year End'].rename(columns={'Value':'Year End'})

tem2 = tem2.groupby('company_externalid')['Year End'].apply(lambda x: '\n'.join(x)).reset_index()
tem2 = tem2.drop_duplicates()

tem3 = note1[['company_externalid','Value']].loc[note1['Detail']=='No Temps'].rename(columns={'Value':'No Temps'})

tem3 = tem3.groupby('company_externalid')['No Temps'].apply(lambda x: '\n'.join(x)).reset_index()
tem3 = tem3.drop_duplicates()

tem4 = note1[['company_externalid','Value']].loc[note1['Detail']=='Hardest Posn'].rename(columns={'Value':'Hardest Posn'})

tem4 = tem4.groupby('company_externalid')['Hardest Posn'].apply(lambda x: '\n'.join(x)).reset_index()
tem4 = tem4.drop_duplicates()

tem5 = note1[['company_externalid','Value']].loc[note1['Detail']=='Rec Methods Used'].rename(columns={'Value':'Rec Methods Used'})

tem5 = tem5.groupby('company_externalid')['Rec Methods Used'].apply(lambda x: '\n'.join(x)).reset_index()
tem5 = tem5.drop_duplicates()

tem6 = note1[['company_externalid','Value']].loc[note1['Detail']=='Referred by'].rename(columns={'Value':'Referred by'})

tem6 = tem6.groupby('company_externalid')['Referred by'].apply(lambda x: '\n'.join(x)).reset_index()
tem6 = tem6.drop_duplicates()

note2 = pd.read_sql("""
select concat('', Company_Reference) as company_externalid
     , Charge_VAT
from DB_Client_System_Details""", engine_mssql)
note2 = note2.drop_duplicates()
note2.loc[note2['Charge_VAT']=='True', 'Charge_VAT'] = 'Yes'
note2.loc[note2['Charge_VAT']=='False', 'Charge_VAT'] = 'No'

note3 = pd.read_sql("""
select concat('', sl.Reference) as company_externalid
     , concat(trim(skill),': ', Skill_Value,'%') as skill
from Skills_Lookup sl
left join (select sd.Reference, concat(trim(sg.Description),' - ', trim(sd.Description)) as skill
from Skill_Description sd
left join Skill_Group sg on sg.Reference = sd.Skill_Group) s on s.Reference = sl.Skill_Reference
where Reference_type = 2""", engine_mssql)
note3 = note3.drop_duplicates()
note3 = note3.groupby('company_externalid')['skill'].apply(lambda x: ', '.join(x)).reset_index()

note4 = pd.read_sql("""
select concat('', kl.Reference) as company_externalid, keyword
from keyword_lookup kl left join
(select q.Reference, concat(trim(k.Description),' - ', trim(q.Description)) as keyword
from Qualifiers q
left join Keyword k on k.Reference = q.Keyword) a on a.Reference = kl.Keyword_Reference
where Reference_type = 2""", engine_mssql)
note4 = note4.drop_duplicates()
note4 = note4.groupby('company_externalid')['keyword'].apply(lambda x: ', '.join(x)).reset_index()

note_5 = note.query('Reference_Type == 2 and FactoidGroup == "Tempaid Details"')
note_5 = note_5[['company_externalid','Value']].loc[note_5['Detail']=='Not Invoice Option'].rename(columns={'Value':'Not Invoice Option'})

note6 = pd.read_sql("""
Select nullif(convert(nvarchar(max),an.Notes),'') as Notes, concat('', dl.Reference) as company_externalid
from Notes an
inner join Details_Lookup dl
on dl.Detail_reference = an.Reference
and an.notes_name = 'Client_Notes'
where dl.Detail_Type = 7
""", engine_mssql)
note6 = note6.dropna().drop_duplicates()
note6.loc[note6['company_externalid']=='2623']
note6 = note6.groupby('company_externalid')['Notes'].apply(lambda x: '\n--------------------\n '.join(x)).reset_index()


tem = company.merge(tem1, on='company_externalid', how='left')
tem = tem.merge(tem2, on='company_externalid', how='left')
tem = tem.merge(tem3, on='company_externalid', how='left')
tem = tem.merge(tem4, on='company_externalid', how='left')
tem = tem.merge(tem5, on='company_externalid', how='left')
tem = tem.merge(tem6, on='company_externalid', how='left')
tem = tem.merge(note2, on='company_externalid', how='left')
tem = tem.merge(note3, on='company_externalid', how='left')
tem = tem.merge(note4, on='company_externalid', how='left')
tem = tem.merge(note_5, on='company_externalid', how='left')
tem = tem.merge(note6, on='company_externalid', how='left')

tem = tem.drop_duplicates()
tem = tem.where(tem.notnull(), None)
tem.info()

tem['note'] = tem[['TOB Signed','Year End', 'No Temps', 'Hardest Posn', 'Rec Methods Used','Referred by','Charge_VAT','skill','keyword','Not Invoice Option','Notes']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TOB Signed','Year End', 'No Temps', 'Hardest Posn', 'Rec Methods Used','Referred by','Charge_VAT','Skills','Keywords','Not Invoice Option','Notes'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
# tem = tem.loc[tem['note']!='']
vcom.update_note_2(tem, dest_db, mylog)

# %% number employee
tem = note1[['company_externalid','Value']].loc[note1['Detail']=='No Staff'].rename(columns={'Value':'employees_number'})
tem = tem.loc[tem['employees_number'] != '0.00']
tem['employees_number'] = tem['employees_number'].astype(float)
vcom.update_employees_number(tem, mylog)

# %% industry
company_industries = pd.read_sql("""
select idcompany as company_externalid, idindustry_string_list from companyx
""", engine_sqlite)
company_industries = company_industries.dropna()
industry = company_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(company_industries[['company_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['company_externalid'], value_name='idIndustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idIndustry'] = industry['idIndustry'].str.lower()
industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()
company_industries = industry.merge(industries, on='idIndustry')
company_industries['value'] = company_industries[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
company_industries['matcher'] = company_industries['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
company_industries = company_industries.merge(industries_csv, on='matcher')

company_industries_2 = company_industries[['company_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
company_industries_2 = company_industries_2.where(company_industries_2.notnull(),None)
tem1 = company_industries_2[['company_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
cp10 = vcom.insert_company_industry(tem1, mylog)

tem2 = company_industries_2[['company_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
cp10 = vcom.insert_company_sub_industry(tem2, mylog)




# # %%
# sql = """
# select c.idcompany as company_externalid
# ,c.companyname, addr.*
# from Company c
# join (select idCompany, isdefault
# , addressline1
# , addressline2
# , addressline3
# , addressline4
# , city
# , postcode
# , pa.country
# from Company_PAddress cp
# left join (select paddress.*, country.value as country
# from paddress
# join country on country.idcountry = paddress.idcountry) pa on cp.idpaddress = pa.idpaddress) addr on addr.idCompany = c.idCompany
# """
# company = pd.read_sql(sql, engine_sqlite)
# company['address'] = company[['addressline1', 'addressline2', 'addressline3', 'addressline4', 'city','postcode','country']] \
#     .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# company = company.loc[company['address'] != '']
# sql = """
# select c.idcompany as company_externalid
# , l.Value as location
# from company c
# left join companyx cx on c.idcompany = cx.idcompany
# left join Location l on l.idLocation = c.idLocation
# """
# company_location = pd.read_sql(sql, engine_sqlite)
# company_location = company_location.loc[~company_location['company_externalid'].isin(company['company_externalid'])]
# company_location = company_location.dropna()
# assert False
#
# # %% billing address
# company_location['address'] = company_location['location']
# cp2 = vcom.insert_company_location_2(company_location, dest_db, mylog)
#
# # %% country
# company_location['country_code'] = company_location.location.map(vcom.get_country_code)
# company_location = company_location.loc[company_location['country_code'] != '']
# company_location['country'] = company_location.location
# cp6 = vcom.update_location_country_2(company_location, dest_db, mylog)