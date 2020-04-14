--Update close job on review 2
select id, head_count_open_date, head_count_close_date
from position_description
where deleted_timestamp is NULL
and external_id ilike 'JOB%' --140966

--MAIN SCRIPT
update position_description
set head_count_close_date = head_count_open_date + interval '3 years'
where deleted_timestamp is NULL
and external_id ilike 'JOB%' --140966

