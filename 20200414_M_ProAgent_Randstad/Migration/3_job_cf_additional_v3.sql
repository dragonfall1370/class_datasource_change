/* From Hoshino-san

If all of the following conditions are matched, please set the value of [HP Public Approval] to "承認/approved" and set the value of [Open to HP] to "Open".
1) "Industry" is selected
2) "Functional expertise" is selected
3) "Sub functional expertise" is selected
4) "Prefecture" is selected
5) "Sector Name (DOVA)" is selected
6) Upper-right action-posting-privacy disclosure is "public"

*/
--Currently existing VC jobs
with job_filter as (select id
	from position_description
	where external_id not ilike 'JOB%' or external_id is NULL --22807
	)
	
--Industry selected
, ind_select as (select distinct id
	--, vertical_id
	from position_description
	where 1=1
	and (external_id not ilike 'JOB%' or external_id is NULL)
	and vertical_id is not NULL --13528
)

, fe_select as (select distinct position_id
	from position_description_functional_expertise
	where 1=1
	and functional_expertise_id is not NULL --868557 | distinct 154056
	and sub_functional_expertise_id is not NULL
	and position_id in (select id from job_filter)
	) --select * from fe_select

, prefecture_select as (
	select distinct additional_id
	--, form_id, field_id, field_value
	from additional_form_values
	where 1=1
	and additional_id in (select id from job_filter)
	and field_id = 1113 --Prefecture
	and field_value is not NULL --152221
	) 

, sector_select as (
	select distinct additional_id
	--, form_id, field_id, field_value
	from additional_form_values
	where 1=1
	and additional_id in (select id from job_filter)
	and field_id = 1120 --Sector Name (DOVA)
	and field_value is not NULL --14037
	) --select * from sector_select

, public_select as (select id
	--, private_job
	from position_description
	where 1=1
	and (external_id not ilike 'JOB%' or external_id is NULL)
	and private_job = 0
	) --5933 rows
	
select i.id
from ind_select i
join fe_select fe on fe.position_id = i.id
join prefecture_select pre on pre.additional_id = i.id
join sector_select se on se.additional_id = i.id
join public_select ps on ps.id = i.id
--3327 rows

-->> SHORTER VERSION <<--
with job_filter as (select id
	from position_description
	where 1=1
	and (external_id not ilike 'JOB%' or external_id is NULL) --22807
	and private_job = 0 --5933 rows
	and vertical_id is not NULL --4122 rows
	)
	
, fe_select as (select distinct position_id
	from position_description_functional_expertise
	where 1=1
	and functional_expertise_id is not NULL --868557 | distinct 154056
	and sub_functional_expertise_id is not NULL
	and position_id in (select id from job_filter)
	) --select * from fe_select --3345 rows

, prefecture_select as (
	select distinct additional_id
	--, form_id, field_id, field_value
	from additional_form_values
	where 1=1
	and additional_id in (select position_id from fe_select)
	and field_id = 1113 --Prefecture
	and field_value is not NULL --152221
	) --select * from prefecture_select --3334 rows

, sector_select as (
	select distinct additional_id
	--, form_id, field_id, field_value
	from additional_form_values
	where 1=1
	and additional_id in (select additional_id from prefecture_select)
	and field_id = 1120 --Sector Name (DOVA)
	and field_value is not NULL --14037
	) --select * from sector_select --3327 rows

-->>#CF | Open to HP | 1048 | Public value
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_job_info' additional_type
, additional_id
, 1003 form_id
, 1048 field_id
, 1 field_value --Public | 公開
from sector_select
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value; --3327 rows


-->>#CF | HP Public Approval | 11317 | Public value
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_job_info' additional_type
, additional_id
, 1003 form_id
, 11317 field_id
, 2 field_value --Approved | 承認
from sector_select
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value; --3327 rows


--LAST UPDATED ON --
select [PANO ] as job_ext_id
, dateadd(hour, -9, convert(datetime, [更新日], 120)) as last_updated_on
from csv_job