--JOB REPORT
select id as "VCID"
, external_id as "PAID"
from position_description
where 1=1
and deleted_timestamp is NULL
and external_id ilike '%JOB%'
order by id