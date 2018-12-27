with 
--Job Function as FE
JobFunctions as (select k_id, listvalue, meta_2
	from common.Lists
	where listkey = 'tblVacancyJobFunctions')

--Business / Industry as Industry
, BusinessIndustry as (select k_id, listvalue, meta_2
	from common.Lists
	where listkey = 'tblLookupJobExpBizCat')

--Reason Leave
, ReasonLeave as (select k_id, listvalue, meta_2
	from common.Lists
	where listkey = 'tblReasonLeave')

--GET MAX REVERSE INDEX AS PRESENT JOB TITLE/COMPANY
, ReverseIndex as (select cn_id
	, row_number() over (partition by cn_id order by job_present_flag asc, job_from asc, job_id desc) as ReverseIndexrn
	from candidate.Jobs 
	)

, MaxReverseIndex as (select cn_id
	, max(ReverseIndexrn) as MaxReverseIndex
	from ReverseIndex
	group by cn_id)

--PRESENT JOB TITLE/COMPANY TAKEN FROM CANDIDATE TABLE >> to cover current position in Candidate Table
, CurrentJob as (select c.cn_id
	, c.cn_present_position
	, c.cn_present_company
	, case when m.MaxReverseIndex is NULL then 1
	else m.MaxReverseIndex + 1 end as ReverseIndexrn
	from candidate.Candidates c
	left join MaxReverseIndex m on m.cn_id = c.cn_id
	where (c.cn_present_position is not NULL and c.cn_present_position <> '')
	or (c.cn_present_company is not NULL and c.cn_present_company <> ''))

--WORK HISTORY
, WorkHistory as (select j.job_id
	, j.cn_id
	, j.job_index
	, row_number() over (partition by j.cn_id order by j.job_present_flag desc, j.job_from desc, j.job_id asc) as Indexrn --update the rule to count job_id as well
	, row_number() over (partition by j.cn_id order by j.job_present_flag asc, j.job_from asc, j.job_id desc) as ReverseIndexrn 
	--change rule due to PRTR requirements, the most recent will be in the last row
	, j.job_present_flag
	, j.job_company_name
	, j.job_position
	, j.job_category
	, f.listvalue as JobFunction
	, j.job_income
	, j.job_salary_currency_text
	, j.job_company_industry
	, b.listvalue as JobIndustry
	, j.job_company_address1 as CompanyBusiness
	, j.job_emp_number
	, j.job_company_address
	, j.job_from
	, j.job_to
	, j.job_boss_position as DirectBoss
	, j.job_reason_for_leaving
	, r.listvalue as ReasonForLeaving	
	, j.job_txtDetails
	/* REQUEST FUNTION TO RUN THE FOLLOWING ROW: [dbo].[udf_StripHTML] & [dbo].[udf_StripCSS] */
	, replace(replace(replace(replace(ltrim([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](j.job_txtDetails))),char(9),''),char(10),''),char(13),''),'.','. ') as JobDesc
	, job_exp_years
	from candidate.Jobs j
	left join JobFunctions f on f.k_id = j.job_category
	left join BusinessIndustry b on b.k_id = j.job_company_industry
	left join ReasonLeave r on r.k_id = j.job_reason_for_leaving

	UNION ALL

	select concat(99,cn_id) as job_id
	, cn_id
	, 0 as job_index
	, 0 as Indexrn
	, ReverseIndexrn
	, 2 as job_present_flag
	, cn_present_company as job_company_name
	, cn_present_position as job_position
	, '' as job_category
	, '' as JobFunction
	, '' as job_income
	, '' as job_salary_currency_text
	, '' as job_company_industry
	, '' as JobIndustry
	, '' as CompanyBusiness
	, '' as job_emp_number
	, '' as job_company_address
	, NULL as job_from
	, NULL as job_to
	, '' as DirectBoss
	, '' as job_reason_for_leaving
	, '' as ReasonForLeaving
	, '' as job_txtDetails
	, concat_ws(char(10)
		, coalesce('Employer: ' + nullif(ltrim(rtrim(cn_present_company)),''),NULL)
		, coalesce('Job Title: ' + nullif(ltrim(rtrim(cn_present_position)),''),NULL)
		) as JobDesc
	, NULL as job_exp_years
	from CurrentJob)

, MaxIndexWorkHistory as (select cn_id
	, max(ReverseIndexrn) as MaxIndexWorkHistory
	from WorkHistory
	group by cn_id)

--MAIN SCRIPT
, MainScript as (
	select w.job_id
	, w.cn_id
	, w.job_index
	, w.ReverseIndexrn
	, job_present_flag
	, case when job_present_flag = 2 or w.ReverseIndexrn = mr.MaxIndexWorkHistory then 1
	else 0 end as cb_Employer
	, w.job_company_name
	, w.job_position
	, w.job_category
	, w.JobFunction
	, w.job_income
	, w.job_salary_currency_text
	, w.job_company_industry
	, w.JobIndustry
	, w.CompanyBusiness
	, w.job_emp_number
	, w.job_company_address
	, w.job_from
	, w.job_to
	, w.DirectBoss
	, w.job_reason_for_leaving
	, w.ReasonForLeaving
	, job_txtDetails
	, trim(JobDesc) as JobDesc
	, job_exp_years
	from WorkHistory w
	left join MaxIndexWorkHistory mr on mr.cn_id = w.cn_id)

/* SAMPLE CANDIDATE

select * 
from MainScript
where cn_id = 10645
order by cn_id desc;

--INSERT INTO TEMP TABLE

select *
into MainScript
from MainScript

*/

-->>> WORK HISTORY SUMMARY <<<--
/* RUN THIS SCRIPT AFTER CREATING TEMP TABLE [MainScript] */
select concat('PRTR',cn_id) as CandidateExtID
, string_agg(
	concat_ws(char(10)
		, coalesce(case when cb_Employer= 1 then 'Current Employer: ' else 'Employer: ' end + nullif(ltrim(rtrim(job_company_name)),''),NULL)
		, coalesce(case when cb_Employer= 1 then 'Current Job Title: ' else 'Job Title: ' end + nullif(ltrim(rtrim(job_position)),''),NULL)
		, coalesce('Company address: ' + nullif(ltrim(rtrim(job_company_address)),''),NULL)
		, coalesce('Job Function: ' + nullif(ltrim(rtrim(JobFunction)),''),NULL)
		, coalesce('Job Industry: ' + nullif(ltrim(rtrim(JobIndustry)),''),NULL)
		, coalesce('What is the Company''s business?: ' + nullif(ltrim(rtrim(CompanyBusiness)),''),NULL)
		, coalesce('No. of Employees: ' + nullif(ltrim(rtrim(job_emp_number)),''),NULL)
		, coalesce('Employment From: ' + nullif(ltrim(rtrim(convert(varchar(10),job_from,120))),''),NULL)
		, coalesce('Employment To: ' + nullif(ltrim(rtrim(convert(varchar(10),job_to))),''),NULL)
		, coalesce('Position of direct boss: ' + nullif(ltrim(rtrim(DirectBoss)),''),NULL)
		, coalesce('Reason for leaving: ' + nullif(ltrim(rtrim(ReasonForLeaving)),''),NULL)
		, coalesce('Job Description: ' + nullif(ltrim(rtrim(JobDesc)),''),NULL)
		), concat(char(10),char(13))) within group (order by ReverseIndexrn desc)
		 as WorkHistorySummary
from MainScript
--where cn_id = 10645
group by cn_id
order by cn_id