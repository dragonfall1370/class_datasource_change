--JOB REPORT
select id as "VCID"
, external_id as "PAID"
from candidate
where 1=1
and deleted_timestamp is NULL
and external_id ilike 'CDT%'
order by id