with holiday as (select additional_id as job_id
	, nullif(replace(replace(replace(replace(field_value, '1', ' Saturday'), '2', ' Sunday'), '3', ' Public Holiday'), '4', ' Other'), '') as job_holidays
	from additional_form_values
	where field_id = 1069
)

, holiday_details as (select additional_id as job_id
	, field_value as job_holidays_details
	from additional_form_values
	where field_id = 1070)
	
select id as "Job VCID"
, h.job_holidays as "Holidays"
, hd.job_holidays_details as "Holidays (Details)"
from position_description pd
left join holiday h on h.job_id = pd.id
left join holiday_details hd on hd.job_id = pd.id
where pd.deleted_timestamp is NULL