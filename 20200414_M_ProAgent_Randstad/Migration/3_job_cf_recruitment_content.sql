--#CF | Age lower limit | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [年齢 下限] as field_value
	, current_timestamp as insert_timestamp
from csv_job
where nullif([年齢 下限], '') is not NULL

UNION ALL
---#CF | Age upper limit | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [年齢 上限] as field_value
	, current_timestamp as insert_timestamp
from csv_job
where nullif([年齢 上限], '') is not NULL


--#CF | Relocation | Radio | 1062
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1062 as field_id
	, 転勤
	, case 
		when 転勤 = '有り' then 'Yes'
		when 転勤 = '無し' then 'No'
		else 'Not Confirmed' end as field_value
	, current_timestamp as insert_timestamp
from csv_job