--Salary from to / default working hours for CONTRACT / TEMPORARY
--PA is 10 thousand JPY, VC is JPY
with compensation as (select [PANO ] as job_ext_id
	, [ポジション名] as job_title
	, [年収 下限] --salary from
	, try_parse([年収 下限] as float using 'ja-JP') as salaryfrom --salary from
	, [年収 上限] --salary to
	, try_parse([年収 上限] as float using 'ja-JP') as salaryto --salary from	
	from csv_job)

---Job Type | Employment Type | 雇用区分
, jobtype as (select [PANO ] as job_ext_id
	, value as jobtype
	from csv_job
	cross apply string_split([雇用区分], char(10))
	where [雇用区分] <> '')

/* Priority job type / employment type before Randstad cleanse
'正社員' then 'PERMANENT' > 1
'契約社員' then 'CONTRACT' > 3
'紹介予定派遣' then 'TEMPORARY_TO_PERMANENT' > 5
'【新卒】　正社員' then 'PERMANENT' > 2
'【新卒】　契約社員' then 'CONTRACT' > 4
'【新卒】　紹介予定派遣' then 'TEMPORARY_TO_PERMANENT' > 6
else 0

'正社員' then 'FULL_TIME' > 1
'契約社員' then 'FULL_TIME' > 3
'紹介予定派遣' then NULL > 5
'【新卒】　正社員' then 'FULL_TIME' > 2
'【新卒】　契約社員' then 'FULL_TIME' > 4
'【新卒】　紹介予定派遣' then NULL > 6
else 0
*/

, jobtype_employment as (select job_ext_id
, case jobtype
	when '正社員' then 1
	when '契約社員' then 3
	when '紹介予定派遣' then 5
	when '【新卒】　正社員' then 2
	when '【新卒】　契約社員' then 4
	when '【新卒】　紹介予定派遣' then 6
	else 0 end as jobtype
, case jobtype 
	when '正社員' then 1
	when '契約社員' then 3
	when '紹介予定派遣' then 5
	when '【新卒】　正社員' then 2
	when '【新卒】　契約社員' then 4
	when '【新卒】　紹介予定派遣' then 6
	else 0 end as employment_type
from jobtype)

, type_employment_rn as (select job_ext_id
	, jobtype
	, row_number() over(partition by job_ext_id order by jobtype asc) as jobtype_rn
	, employment_type
	, row_number() over(partition by job_ext_id order by employment_type asc) as employmentype_rn
	from jobtype_employment
	where jobtype > 0 and employment_type > 0)

, final_jobtype as (select job_ext_id
, case 
	when jobtype in (1, 2) then 'PERMANENT'
	when jobtype in (3, 4) then 'CONTRACT'
	when jobtype in (5, 6) then 'TEMPORARY_TO_PERMANENT'
	else 'PERMANENT' end as jobtype
, case 
	when employment_type in (1, 2) then 'FULL_TIME'
	when employment_type in (3, 4) then 'FULL_TIME'
	else NULL end as employment_type
from type_employment_rn
where jobtype_rn = 1)

select compensation.job_ext_id
, job_title
, coalesce(fj.jobtype, 'PERMANENT') as jobtype
, coalesce(salaryfrom * 10000, 0) as salaryfrom --contract_rate_from
, coalesce(salaryto * 10000, 0) as salaryto --contract_rate_to
, case when salaryfrom is not NULL and salaryto is not NULL then (salaryfrom + salaryto) / 2 * 10000
	else coalesce(nullif(salaryfrom, ''), nullif(salaryto, '')) * 10000 end as annual_salary
--Default working hours for contract
, 8 as working_hours_per_day
, 5 as working_days_per_week
, 40 working_hours_per_week
, 22 working_days_per_month
, 4 working_weeks_per_month
, 'JP' as country_code
from compensation
left join final_jobtype fj on fj.job_ext_id = compensation.job_ext_id
where coalesce(nullif(salaryfrom, ''), nullif(salaryto, '')) is not NULL
and fj.jobtype in ('CONTRACT', 'TEMPORARY_TO_PERMANENT')