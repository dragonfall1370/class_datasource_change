--#CF Holidays | Multiple selection | 1069
with job_holiday as (select distinct [PANO ] as job_ext_id
	, value as job_holiday
	from csv_job
	cross apply string_split(休日, char(10))
	where 1=1 and 休日 <> ''
	--and [PANO ] = 'JOB011870'
)

select job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1069 as field_id
	, job_holiday
	, case 
		when job_holiday = '土曜' then 'Saturday'
		when job_holiday = '日曜' then 'Sunday'
		when job_holiday = '祝日' then 'Public holiday'
		when job_holiday = 'その他' then 'Other'
		else NULL end as field_value
	, current_timestamp as insert_timestamp
from job_holiday
where job_holiday is not NULL


--#CF Holidays (Details) | Text Area | 1070
with holiday_details as (select [PANO ] as job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 1070 as field_id
, concat_ws(concat(char(10), char(13))
	, coalesce('【有給休暇 初年度】' + char(10) + nullif([有給休暇 初年度], ''), NULL) --paid holidays first year
	, coalesce('【有給休暇 発生月】' + char(10) + '入社' + nullif([有給休暇 発生月] + 'ヶ月後', ''), NULL) --paid holidays (after joining)
	, coalesce('【年間休日】' + char(10) + nullif([年間休日], ''), NULL) --annual holiday
	, coalesce('【休日メモ】' + char(10) + nullif([休日メモ], ''), NULL) --holiday note
	, coalesce('【休暇】' + char(10) + '■' + nullif(replace([休暇], char(10), concat(char(10),'■')),''), NULL) --vacation
	, coalesce('【休暇メモ】' + char(10) + nullif([休暇メモ], ''), NULL) --vacation memo
	, coalesce('【有給休暇メモ】' + char(10) + nullif([有給休暇メモ], ''), NULL) --paid vacation note
	) as field_value
, current_timestamp as insert_timestamp
from csv_job)

select *
from holiday_details
where field_value <> ''