with open_enddate as (select [Job PANo]
		, coalesce(nullif(convert(date, [Open], 120),''), NULL) as open_date
		, coalesce(nullif(convert(date, [Close], 120),''), NULL) as end_date
		, coalesce(nullif(convert(date, [Other], 120),''), NULL) as other_date
		--into open_enddate
		from csv_Job_Situation)

select [Job PANo] as job_ext_id
, convert(datetime, other_date, 120) as submission_date
from open_enddate
where nullif(other_date, '') is not NULL