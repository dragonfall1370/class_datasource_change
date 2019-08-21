--CANDIDATE WORK HISTORY (CUSTOM TABLE - CUSTOM FIELDS)
with WHSalary as (
select cn_id
, job_company_name
, job_income
, job_salary_currency
, row_number() over (partition by cn_id order by job_present_flag desc, job_from desc, job_id asc) - 1 as Indexrn
, row_number() over (partition by cn_id order by job_present_flag asc, job_from asc, job_id desc) as ReverseIndexrn 
from candidate.Jobs
where job_income > 0)

--COMPANY NAME
, CandidateSalary as (
select concat('PRTR',cn_id) as CandidateExtID
, 1090 as parent_id
, 1091 as children_id
, job_company_name as text_data
, ReverseIndexrn
, getdate() as insert_timestamp
, '1091_1090' as constraint_id
from WHSalary

UNION ALL

--INCOME SALARY
select concat('PRTR',cn_id) as CandidateExtID
, 1090 as parent_id
, 1092 as children_id
, cast(job_income as nvarchar(max)) as text_data
, ReverseIndexrn
, getdate() as insert_timestamp
, '1092_1090' as constraint_id
from WHSalary

UNION ALL

--SALARY CURRENCY
select concat('PRTR',cn_id) as CandidateExtID
, 1090 as parent_id
, 1093 as children_id
, job_salary_currency as text_data
, ReverseIndexrn
, getdate() as insert_timestamp
, '1093_1090' as constraint_id
from WHSalary)

select CandidateExtID
, parent_id
, children_id
, text_data
, Indexrn
, insert_timestamp
, constraint_id
from CandidateSalary
order by CandidateExtID asc, parent_id asc, ReverseIndexrn asc, children_id asc