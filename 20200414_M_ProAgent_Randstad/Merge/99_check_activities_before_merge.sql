--CHECK CANDIDATE ACTIVITIES
with activity_rn as (select id, candidate_id, content
, row_number() over(partition by candidate_id, content order by id desc) as rn
from activity
where candidate_id in (select id from candidate where external_id ilike 'CDT%'))

select * from activity_rn 
where rn > 1
and id > 698207 --after fixing duplicate activities


--CHECK COMPANY ACTIVITIES
with activity_rn as (select id, company_id, content
, row_number() over(partition by company_id, content order by id desc) as rn
from activity
where company_id in (select id from company where external_id ilike 'CPY%'))

select * from activity_rn 
where rn > 1
and id > 240546


select count(*)
from activity
where id between 240546 and 698207
and company_id > 0 --54125