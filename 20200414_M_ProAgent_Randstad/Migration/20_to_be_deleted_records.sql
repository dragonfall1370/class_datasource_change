--To be deleted records
select id, name
from company
where id in (select distinct vc_pa_company_id from mike_tmp_company_dup_check)
order by id --6243


select id, first_name, last_name, email
from contact
where id in (select distinct contact_id from mike_tmp_contact_dup_check)
order by id --2485 | distinct 2472


select *
from mike_tmp_contact_dup_check
where contact_id in (select contact_id from mike_tmp_contact_dup_check group by contact_id having count(*) > 1)
order by contact_id


select id, first_name, last_name, email
from candidate
where id in (select distinct vc_pa_candidate_id from mike_tmp_candidate_dup_check)
order by id


---Records related to-be-deleted records
--CONTACT BOUND WITH TO-BE-DELETED COMPANIES
with contact_after_dm as (select c.id as contact_id
	, c.company_id
	, c.first_name
	, c.last_name
	, c.insert_timestamp
	, c.job_title
	, u.name as user_name
	from contact c
	left join user_account u on u.id = c.user_account_id
	where company_id in (select vc_pa_company_id from mike_tmp_company_dup_check) --17 rows
)

--JOB LINKS WITH CONTACTS AFTER MIGRATION
, job_after_dm as (select id, name, contact_id, company_id, insert_timestamp
	from position_description pd
	where contact_id in (select contact_id from contact_after_dm) --18 rows
) --select * from job_after_dm

---COMPANY ACTIVITIES
, com_activity_after_dm as (select id, company_id, content
	from activity
	where company_id in (select vc_pa_company_id from mike_tmp_company_dup_check)
	and id  > 695989
	and content <> '' --5 activities
	)
	
, con_activity_after_dm as (select id, contact_id, content
	from activity
	where contact_id in (select contact_id from contact_after_dm)
	and id  > 695989
	and content <> '' --8 activities
	)
	
--JOB ACTIVITIES WITH RECORDS AFTER DM
, job_activity_after_dm as (select id, position_id, content
	from activity
	where position_id in (select id from job_after_dm)
	and id  > 695989
	and content <> '' --2051 activities
	)

--JOB APPLICATION WITH RECORDS AFTER DM	
select id as job_app_id
, position_description_id
, candidate_id
, associated_date
, status
from position_candidate
where position_description_id in (select id from job_after_dm)



--COMPANY ACTIVITIES
select id, company_id, insert_timestamp, content
from activity
where company_id in (select vc_pa_company_id from mike_tmp_company_dup_check)
and id  > 695989
and company_id > 0
and content <> ''

--CONTACT ACTIVITIES
select id, contact_id, insert_timestamp, content
from activity
where contact_id in (select contact_id from mike_tmp_contact_dup_check)
and id  > 695989
and contact_id > 0
and content <> ''


--CANDIDATE ACTIVITIES
select id, candidate_id, insert_timestamp, content
from activity
where candidate_id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)
and id  > 695989
and candidate_id > 0
and content <> ''