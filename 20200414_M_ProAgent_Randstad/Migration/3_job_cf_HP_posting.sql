---#CF | Open to HP | RADIO_BUTTON
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1048 as field_id
	, case when [HP公開] = '公開' then 'Open' --'Release'
		when [HP公開] = '非公開' then 'Closed' --'Private'
		else NULL end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where nullif([HP公開], '') is not NULL


---#CF | Display HP company name | RADIO_BUTTON
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id --Display HP company name
	, [社名表示]
	, case when [社名表示] = '実名' then 'Yes'
		else NULL end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where nullif([社名表示], '匿名') is not NULL --Only applied for value '実名'


---#CF | HP public approval flag | RADIO_BUTTON
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, HP公開承認フラグ --HP public approval flag
	, 募集状況 --status
	, case when [募集状況] = 'Open' and [HP公開承認フラグ] = '承認' then '公開/Public'
		else NULL end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where 1=1
and [募集状況] = 'Open'
and [HP公開承認フラグ] = '承認'


---#CF | RS office | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id --RS office
	, [RS担当オフィス] as field_value --free text
	, current_timestamp as insert_timestamp
from csv_job
where nullif([RS担当オフィス], '') is not NULL


---#CF | JOB reception date | Free text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id --JOB reception date
	, [JOB受付年月日] as field_value --free text
	, current_timestamp as insert_timestamp
from csv_job
where nullif([JOB受付年月日], '') is not NULL