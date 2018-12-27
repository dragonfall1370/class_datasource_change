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

, ReverseIndex as (select cn_id
	, row_number() over (partition by cn_id order by job_present_flag asc, job_from asc, job_id desc) as RowIndex
	from candidate.Jobs 
	)

, MaxReverseIndex as (select cn_id --In case, job_present_flag is not marked as present job > take the latest one
	, max(RowIndex) as MaxRowIndex
	from ReverseIndex
	group by cn_id)

--MAIN SCRIPT
, MainScript as (
	select j.job_id
	, j.cn_id
	, j.job_index
	, row_number() over (partition by j.cn_id order by j.job_present_flag desc, j.job_from desc, j.job_id asc) as Indexrn --update the rule to count job_id as well
	, row_number() over (partition by j.cn_id order by j.job_present_flag asc, j.job_from asc, j.job_id desc) as ReverseIndexrn 
	--change rule due to PRTR requirements, the most recent will be in the last row
	, case when j.job_present_flag = 1  
	or row_number() over (partition by j.cn_id order by j.job_present_flag asc, j.job_from asc, j.job_id desc) = mr.MaxRowIndex then 1
	else 0 end as cb_Employer
	, j.job_company_name
	, j.job_position
	, j.job_category
	, f.listvalue as JobFunction
	, j.job_income
	, j.job_salary_currency_text
	, j.job_company_industry
	, b.listvalue as JobIndustry
	/* REQUEST FUNTION TO RUN THE FOLLOWING ROW: [dbo].[udf_StripHTML] & [dbo].[udf_StripCSS]
	, j.job_txtDetails
	, replace(replace(replace(replace(ltrim([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](j.job_txtDetails))),char(9),''),char(10),''),char(13),''),'.','. ')
	*/
	, j.job_company_address1 as CompanyBusiness
	, j.job_emp_number
	, j.job_company_address
	, j.job_from
	, j.job_to
	, j.job_boss_position as DirectBoss
	, j.job_reason_for_leaving
	, r.listvalue as ReasonForLeaving
	/* REQUEST FUNTION TO RUN THE FOLLOWING ROW: [dbo].[udf_StripHTML] & [dbo].[udf_StripCSS] */
	, j.job_txtDetails
	--, replace(replace(replace(replace(ltrim([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](j.job_txtDetails))),char(9),''),char(10),''),char(13),''),'.','. ') as JobDesc
	, replace(replace(trim([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](j.job_txtDetails))),char(9),''),'.','. ') as JobDesc
	, job_exp_years
	from candidate.Jobs j
	left join JobFunctions f on f.k_id = j.job_category
	left join BusinessIndustry b on b.k_id = j.job_company_industry
	left join ReasonLeave r on r.k_id = j.job_reason_for_leaving
	left join MaxReverseIndex mr on mr.cn_id = j.cn_id
	)

/* INSERT INTO TEMP TABLE

select *
into MainSript
from MainScript
order by cn_id desc

*/

-->>> WORK HISTORY SUMMARY <<<--
/* RUN THIS SCRIPT AFTER CREATING TEMP TABLE [MainScript] */
select concat('PRTR',cn_id) as CandidateExtID
, string_agg(concat_ws(char(10)
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
		), char(10)) within group (order by ReverseIndexrn desc) as WorkHistorySummary
from MainScript
group by cn_id
order by cn_id