select count(*) from candidate.Candidates --296418
where can_type = 2 --62297

select distinct can_type from candidate.Candidates --1/2

select count(*) from candidate.Candidates where company_id > 0 --62258
and company_id in (select company_id from company.Companies) --62246

select * from common.History

select distinct contact_id 
from common.History --61714
where contact_id in (select cn_id from candidate.Candidates) --4151

select count(*)
from candidate.ContactsView --296418

select count(*)
from candidate.CandidatesView --296418

select distinct can_type from candidate.Candidates where company_id > 0 --62258
and company_id in (select company_id from company.Companies) --62246

select * from candidate.Candidates
where company_id > 0
and can_type = 2

--Job with distinct contact type
select distinct c.can_type 
from vacancies.Vacancies v --39154
left join candidate.Candidates c on v.contact_id = c.cn_id --can_type = 2 | NULL

--Job with contacts not listed in candidate.Candidates table
select *
from vacancies.Vacancies v --39154
where v.contact_id not in (select cn_id from candidate.Candidates) --4

-->> Analysis for case above
select * from company.Companies where company_id = 1018287

select * from candidate.Candidates where company_id = 1018287

--==================
select distinct cn_sex
from candidate.Candidates

select distinct cn_salut_text
from candidate.Candidates

select cn_fname
from candidate.Candidates

--SEARCH ALL TABLES OR COLUMNS NAMES (SQL server)
SELECT distinct table_name, COLUMN_NAME, TABLE_SCHEMA
FROM INFORMATION_SCHEMA.COLUMNS
WHERE 1=1
--and TABLE_NAME = 'YourTableName' 
--AND TABLE_SCHEMA='dbo'
and COLUMN_NAME like '%name%'
--and table_name like '%name%'

select top 100 cn_cont_email3 from candidate.Candidates
where can_type = 2
and cn_others_FULL is not NULL

select distinct doc_class from common.Documents

select distinct comment_class from common.Comments
/* 
Candidate
Company
Contact
Placement
Vacancy
*/

select * from common.Comments
where comment_class = 'Contact' --talked to K.Kaew and she would like to get the salary information for the Account Manager position.

select * from candidate.Candidates
where cn_id = 5166787

select * from common.History
where his_action = 'Contact Added'
and company_id = 133
--and his_id = 19643
and his_details = 'Yupadee Vasayangkura'

select * from candidate.Candidates
where cn_id = 1004739

select * from common.History
where contact_id not in (select cn_id from candidate.Candidates)


/* ==================== CANDIDATE WORK HISTORY ==================== */

--CANDIDATE SALARY
select cn_present_salary, cn_salary_sought
from candidate.Candidates
where can_type = 1

--CANDIDATE WORK HISTORY
select cn_id, cn_position_sought, cn_present_position, cn_present_company, cn_job_companies, cn_job_positions, cn_present_function, cn_present_position1, cn_job_function
from candidate.Candidates
where cn_id = 10718;

select *
from candidate.Jobs
where cn_id = 10718
order by job_index;

select distinct listkey
from common.Lists

---job_company_industry | LookupJobExpBizCat | Business / Industry & INDUSTRY
select *
from common.Lists
where listkey = 'tblLookupJobExpBizCat'

select cn_id, cn_job_company_industry, cn_job_business_industries
from candidate.Candidates
where cn_id = 10718

select j.*, l.*
from candidate.Jobs j
left join common.Lists l on l.k_id = j.job_company_industry and l.listkey = 'tblLookupJobExpBizCat'
where j.cn_id = 10718
order by j.job_index;

---job_category | VacancyJobFunctions | Job Function & FUNCTIONAL EXPERTISE
select *
from common.Lists
where listkey = 'tblVacancyJobFunctions'

select distinct job_category
from candidate.Jobs

select cn_id, cn_job_function
from candidate.Candidates
where cn_id = 10718

select j.*, l.*
from candidate.Jobs j
left join common.Lists l on l.k_id = j.job_category and l.listkey = 'tblVacancyJobFunctions'
where j.cn_id = 10718
order by j.job_index;

---job_other_leaving_reason | ReasonLeave | Reason for Leaving
select *
from common.Lists
where listkey = 'tblReasonLeave'

select j.job_other_leaving_reason, j.job_reason_for_leaving, j.*, l.*
from candidate.Jobs j
left join common.Lists l on l.k_id = j.job_reason_for_leaving and l.listkey = 'tblReasonLeave'
where j.cn_id = 10718
order by j.job_index;


/* ==================== JOB APPLICATION ============================== */
--1
select *
from placements.Placements
where can_id = 5107980 --vac_id = 1000402

--2
select * from vacancies.Shortlist
where can_id = 25058
and vac_id = 1000402 --shortlist_id = 19498 

select distinct movement from vacancies.Shortlist

select *
from vacancies.Shortlist
where movement = 'Direct Applicants' --can_id = 5107980 and vac_id = 1088650 --910941

select *
from vacancies.Shortlist
where movement = 'Offer' --shortlist_id = 934947 | can_id = 27535 and vac_id = 1088628

select *
from vacancies.Shortlist
where can_id = 27535 and vac_id = 1088628

--3
select * from vacancies.ShortlistTracking
where shortlist_id = 19498
order by timestamp

select * from vacancies.ShortlistTracking
where shortlist_id = 910941
order by timestamp

select * from vacancies.ShortlistTracking
where shortlist_id = 934947
order by timestamp

select cn_id, cn_present_salary
, case when isnumeric(cn_present_salary) = 1 then cn_present_salary * 12
else NULL end
from candidate.Candidates
where can_type = 1
order by cn_id

select cn_id, cn_present_salary
, case when isnumeric(cn_present_salary) = 1 then cn_present_salary * 12
else NULL end
from candidate.Candidates
where can_type = 1
order by cn_id --54191 68975	49199	590388

select cn_id, cn_present_salary
, case when isnumeric(cn_present_salary) = 1 then left(cn_present_salary,8) * 12
else NULL end
from candidate.Candidates
where cn_id = 68979
--where can_type = 1
order by cn_id

select cn_id, cn_present_salary from candidate.Candidates where len(cn_present_salary) > 8

select cn_id, cn_salary_sought from candidate.Candidates where len(cn_salary_sought) > 8

select top 10 * from vacancies.ShortlistTracking
where shortlist_id = 882531

select top 10 * from vacancies.Vacancies
where vac_id = 1086932

select top 10 * from vacancies.Shortlist
where vac_id = 1086669 and can_id = 5078051

select top 10 * from placements.Placements
where vac_id = 1086669 and can_id = 5078051

select distinct mov_id from vacancies.ShortlistTracking

select distinct movement from vacancies.Shortlist

select distinct pl_status from placements.Placements

select count(*) from placements.Placements --16697

select distinct vac_id, can_id from placements.Placements
where vac_id > 0 and can_id > 0 --7458

select * from placements.Placements
where vac_id = 1086669

select count( *) from vacancies.Shortlist where vac_id = 1086669

/* ==================== JOB APPLICATION ============================== */