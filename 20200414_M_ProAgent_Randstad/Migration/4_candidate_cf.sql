---#CF PANO No. | Free text
select [PANO ] as cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 9999 as field_id
	, [PANO ] as field_value
	, current_timestamp as insert_timestamp
from csv_can


--Reg date
select [PANO ] as cand_ext_id
, convert(datetime, 登録日, 120) as reg_date
from csv_can


---#CF | AG Human Resources | Checkbox
select distinct [PANO ] as cand_ext_id
, 'add_cand_info' as additional_type
, 1139 as form_id
, 9999 as field_id
, trim(value) as field_value
, current_timestamp as insert_timestamp
from csv_can
cross apply string_split([チェック項目], char(10)) --AG Human Resources
where coalesce(nullif([チェック項目],''), NULL) is not NULL
and value like '%AG人材%' --only take AG Personnel


--#CF | Cognitive pathway | Free text
select distinct [PANO ] as cand_ext_id
, 'add_cand_info' as additional_type
, 1139 as form_id
, 9999 as field_id
, trim(認知経路) as field_value --Cognitive pathway
, current_timestamp as insert_timestamp
from csv_can
where nullif([認知経路],'') is not NULL


--#Inject Met/Not Met
select [キャンディデイト PANO ] as cand_ext_id
, [面談実施実施日] --面談実施実施日
, case when nullif(面談実施実施日, '') is not NULL then 1 --Met
	else 2 end as met_notmet --Not met
from csv_can_history


--#Inject Source Contact
select [キャンディデイト PANO ] as cand_ext_id
, '小野澤 直美' as source_contact --Naomi Onozawa
, 'naomi.onozawa@randstad.co.jp' as source_contact_email
, 29092 as source_contact_id
from csv_can_history

-->> from Vincere
select id, source_contact_id
from candidate
where external_id is not NULL
and deleted_timestamp is NULL
and source_contact_id is NULL

update candidate
set source_contact_id = 29092
where external_id is not NULL
and deleted_timestamp is NULL
and source_contact_id is NULL

--#CF Gender
select [PANO ] as cand_ext_id
, 'add_cand_info' as additional_type
, 1139 as form_id
, 9999 as field_id
, case when 性別 = '男性' then 'Men' --gender
	when 性別 = '女性' then 'Women'
	else NULl end as field_value
from csv_can
where nullif(性別, '') is not NULL