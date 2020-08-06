-->> CANDIDATE
---ADD DELETED TIMESTAMP
select id, external_id, insert_timestamp, deleted_timestamp
from candidate
where id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)
and id != 291480


--UPDATE DELETED TIMESTAMP
update candidate
set deleted_timestamp = current_timestamp
where id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)
and id != 291480


-->> CONTACT - SLAVE DATA
select id, first_name, last_name, email, insert_timestamp, deleted_timestamp
from contact
where id in (select distinct contact_id from mike_tmp_contact_dup_check)
order by id --2472

update contact
set deleted_timestamp = current_timestamp
where id in (select distinct contact_id from mike_tmp_contact_dup_check)


-->> JOB APP - NEW AND RELATED WITH NEWLY ADDED CONTACTS
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
) 

--JOB APPS
select *
--into mike_bkup_newly_added_position_candidate_20200716
from position_candidate
where position_description_id in (select id from job_after_dm)


--MAIN SCRIPT
delete from position_candidate
where position_description_id in (select id from job_after_dm)


--JOBS
select *
into mike_bkup_newly_added_position_description_20200716
from position_description
where id in (select id from job_after_dm)


--->> CONTACT SPECIAL CASE <<---
select *
from contact
where id = 68570 --浩子 | 川村

update contact
set company_id = 13989
where id = 68570

select *
from position_description
where contact_id = 68570


select id, external_id
from company
where id = 43046

select *
from mike_tmp_company_dup_check
where vc_pa_company_id = 43046

select *
from contact
where company_id = 13989

-->> REVERT JOB APP DELETED <<--
insert into position_candidate
select *
from mike_bkup_newly_added_position_candidate_20200716
where position_description_id in (241015, 241030)

--UPDATE JOB COMPANY_ID
select * 
from position_description
where contact_id = 68570


update position_description
set company_id = 13989
where contact_id = 68570
and id in (241015, 241030)


--AUDIT FOR SPECIAL CASES
select id, external_id, *
from contact
where id in (68647, 68662, 68532) --3 distinct contacts


select id, external_id, *
from company
where id in (42612, 62435, 63383) --3 distinct companies

select * from mike_tmp_company_dup_check
where vc_pa_company_id in (42612, 62435, 63383)


--->> JOBS RELATED TO NEWLY ADDED CONTACTS <<---
with contact_after_dm as (select c.id as contact_id
	, c.company_id
	, c.first_name
	, c.last_name
	, c.insert_timestamp
	, c.job_title
	, u.name as user_name
	, external_id
	from contact c
	left join user_account u on u.id = c.user_account_id
	where company_id in (select vc_pa_company_id from mike_tmp_company_dup_check) --17 rows
)

--JOB LINKS WITH CONTACTS AFTER MIGRATION
, job_after_dm as (select id, name, contact_id, company_id, insert_timestamp, external_id, company_id_bkup
	from position_description pd
	where contact_id in (select contact_id from contact_after_dm) --18 rows
) 

select *
--into mike_bkup_newly_added_position_description_20200716
from position_description
where id in (select id from job_after_dm) --16 rows


select *
--into mike_bkup_newly_added_compensation_20200716
from compensation
where position_id in (select id from mike_bkup_newly_added_position_description_20200716)


select *
--into mike_bkup_newly_added_position_agency_consultant_20200716
from position_agency_consultant
where position_id in (select id from mike_bkup_newly_added_position_description_20200716)


--MAIN SCRIPT
delete from position_agency_consultant
where position_id in (select id from mike_bkup_newly_added_position_description_20200716)

delete from compensation
where position_id in (select id from mike_bkup_newly_added_position_description_20200716)

delete from position_description
where id in (select id from mike_bkup_newly_added_position_description_20200716)


--->> NEWLY ADDED CONTACTS <<---
with contact_after_dm as (select c.id as contact_id
	, c.company_id
	, c.first_name
	, c.last_name
	, c.insert_timestamp
	, c.job_title
	, u.name as user_name
	, external_id
	from contact c
	left join user_account u on u.id = c.user_account_id
	where company_id in (select vc_pa_company_id from mike_tmp_company_dup_check) --17 rows
)

--MAIN SCRIPT
update contact
set deleted_timestamp = current_timestamp
where id in (select distinct contact_id from contact_after_dm) --13 rows


---> COMPANIES TO BE DELETED <<---
update company
set deleted_timestamp = current_timestamp
where id in (select distinct vc_pa_company_id from mike_tmp_company_dup_check)


---> UPDATE TOTAL JOBS COUNTS FOR DELETED JOBS <<---
with cand_new_total_job_counts as (select candidate_id, count(*) as new_total_jobs
	from position_candidate
	where candidate_id in (select distinct candidate_id from mike_bkup_newly_added_position_candidate_20200716)
	group by candidate_id)
	
	
select id, total_jobs, new_total_jobs
from candidate c
join cand_new_total_job_counts cn on cn.candidate_id = c.id --no job counts updated