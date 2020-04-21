--Index in related tables
insert into activity_contact (activity_id, contact_id, insert_timestamp)
select id
, contact_id
, insert_timestamp
from activity
where id > 121
and contact_id > 0


insert into activity_candidate (activity_id, candidate_id, insert_timestamp)
select id
, candidate_id
, insert_timestamp
from activity
where id > 121
and candidate_id > 0


insert into activity_company(activity_id, company_id, insert_timestamp)
select id
, company_id
, insert_timestamp
from activity
where id > 121
and company_id > 0


insert into activity_job (activity_id, job_id, insert_timestamp)
select id
, position_id
, insert_timestamp
from activity
where id > 121
and position_id > 0