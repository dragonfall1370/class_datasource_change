--Deleted Open to HP jobs in injection date
select *
from additional_form_values
where field_id = 1048
and insert_timestamp between '2020-07-04' and '2020-07-06'
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL) --134649

--Deleted Open to HP jobs 06-Jul-2020
select *
from additional_form_values
where field_id = 1048
and insert_timestamp between '2020-07-06' and '2020-07-07' 
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL) --9462


---RE-EXECUTE OPEN TO HP
---#CF | Open to HP (step 1) | RADIO_BUTTON
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1048 as field_id
	, case when [HP公開] = '公開' then 'Open' --'Release'
		when [HP公開] = '非公開' then 'Closed' --'Private'
		--else NULL 
		end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where nullif([HP公開], '') is not NULL


--#CF | Open to HP (step 2) | Drop down
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1048 as field_id
	, case when [募集状況] = 'Close' then 'Closed' --'Private' | 非公開 <> 'Open' | '公開'
		--else NULL 
		end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where [募集状況] = 'Close'


--OPEN TO HP > OPEN
select *
from additional_form_values
where field_id = 1048
and field_value = '1' --open
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL)