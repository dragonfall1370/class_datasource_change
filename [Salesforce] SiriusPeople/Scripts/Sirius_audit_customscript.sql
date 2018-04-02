--AUDIT DATA FOR SIRIUS PEOPLE
select * from activity
where type = 'job'
and position_id > 0
and content like 'Job Application ID%' --2649 rows


select * from activity_job
where exists (select id from activity
where activity.id = activity_job.activity_id
and type = 'job'
and position_id > 0
and content like 'Job Application ID%') --2649 rows

select * from activity_job --3871

-->>
delete from activity_job
where exists (select id from activity
where activity.id = activity_job.activity_id
and type = 'job'
and position_id > 0
and content like 'Job Application ID%')

delete from activity
where type = 'job'
and position_id > 0
and content like 'Job Application ID%'

-----
select count(*) from activity
where company_id > 0
and content like 'Subject:%'
and type = 'company' --356203

select count(*) from activity_company --347596
-----
select * from company where business_number is not NULL

delete from additional_form_values
where additional_type = 'add_comp_info'
-----
select count(*) from activity
where contact_id > 0 --109988 // 112754

select count(*) from activity_contact 
where contact_id > 0 --110010 // 112779

-----
select count(*)
from activity
where candidate_id > 0 --250631

select count(*)
from activity_candidate
where candidate_id > 0 --234392

---
select * from company_industry --12 -->>after: 7869

---
select count(*)
from activity
where position_id > 0 --32254

select count(*)
from activity_job
where job_id > 0 --1232

----
select * from contact_industry --43 -->>after: 12396

---
select * from position_description_functional_expertise -->after: 29

---
select * from contact_functional_expertise --6668 / after: 19339

----
