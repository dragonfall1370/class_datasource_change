--#CF | Location (Details) | Text Area | 1102
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1102 as field_id
	, trim([勤務地 詳細]) as field_value --Location Details
	, current_timestamp as insert_timestamp
from csv_job
where nullif([勤務地 詳細], '') is not NULL


--#CF Work location | Work location (department name / address, location number) | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, trim([勤務地（部署名・所在地）※求人管理簿用]) as field_value --Work location
	, current_timestamp as insert_timestamp
from csv_job
where nullif([勤務地（部署名・所在地）※求人管理簿用], '') is not NULL
