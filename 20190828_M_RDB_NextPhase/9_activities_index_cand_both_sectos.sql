--Activities index for candidates in both sectors
select count(*)
from activity
where id > 834195 --813 rows

insert into activity_company(activity_id, company_id, insert_timestamp)
select id
, company_id
, insert_timestamp
from activity
where id > 834195 --deleted activity_id
and company_id > 0 --184902 rows
and id not in (select activity_id from activity_company)


insert into activity_contact (activity_id, contact_id, insert_timestamp)
select id
, contact_id
, insert_timestamp
from activity
where id > 834195
and contact_id > 0 --214012 rows
and id not in (select activity_id from activity_contact)


insert into activity_job (activity_id, job_id, insert_timestamp)
select id
, position_id
, insert_timestamp
from activity
where id > 834195
and position_id > 0 --23989 rows
and id not in (select activity_id from activity_job)

insert into activity_candidate (activity_id, candidate_id, insert_timestamp)
select id
, candidate_id
, insert_timestamp
from activity
where id > 834195
and candidate_id > 0 --139955 rows
and id not in (select activity_id from activity_candidate)