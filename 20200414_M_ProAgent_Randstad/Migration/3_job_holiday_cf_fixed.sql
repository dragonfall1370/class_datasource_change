--#CF Holidays | Multiple selection | 1069
with job_holiday as (select distinct [PANO ] as job_ext_id
	, value as job_holiday
	from csv_job
	cross apply string_split(休日, char(10))
	where 1=1 and 休日 <> ''
	--and [PANO ] = 'JOB011870'
)

, job_holiday_group as (select job_ext_id
	, string_agg(case 		
		when job_holiday = '土曜' then '1'
		when job_holiday = '日曜' then '2'
		when job_holiday = '祝日' then '3'
		when job_holiday = 'その他' then '4'
		end, ',') as field_value
	from job_holiday
	where job_holiday is not NULL
	group by job_ext_id
	) --select * from job_holiday_group

select job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1069 as field_id
	, field_value
	, current_timestamp as insert_timestamp
from job_holiday_group
--and job_ext_id in ('JOB029874')