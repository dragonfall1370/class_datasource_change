---#CF | PA No. | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [PANO ] as field_value --free text
	, current_timestamp as insert_timestamp
from csv_job


--#CF Academic background | Text Area | 
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, trim(学歴) as field_value --Academic background
	, current_timestamp as insert_timestamp
from csv_job
where nullif(学歴, '') is not NULL


--#CF Expected Experience | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1057 as field_id
	, trim([経験]) as field_value --Expect Experience
	, current_timestamp as insert_timestamp
from csv_job
where nullif([経験], '') is not NULL


--#CF Required Skills | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1063 as field_id
	, trim([スキルメモ]) as field_value --Required Skills
	, current_timestamp as insert_timestamp
from csv_job
where nullif([スキルメモ], '') is not NULL


--#CF Licence Requirement | Change to Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 11273 as field_id
	, trim([免許・資格メモ]) as field_value --Licence Requirement
	, current_timestamp as insert_timestamp
from csv_job
where nullif([免許・資格メモ], '') is not NULL


--#CF Expected language skills | Change to Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1065 as field_id
	, trim([語学力メモ]) as field_value --Expected language skills
	, current_timestamp as insert_timestamp
from csv_job
where nullif([語学力メモ], '') is not NULL