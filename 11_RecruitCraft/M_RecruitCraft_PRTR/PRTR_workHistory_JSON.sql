/* SPECIAL NOTES FOR JSON
--Backspace is replaced with \b
--Form feed is replaced with \f
--Newline is replaced with \n
--Carriage return is replaced with \r
--Tab is replaced with \t
--Double quote is replaced with \"
--Backslash is replaced with \\
*/

/* RUN THIS SCRIPT IF WORK HISTORY [MainScript] TABLE WAS ADDED -- currently version 2 */
	
select distinct concat('PRTR',cn_id) as CandidateExtID
, getdate() as insert_timestamp --removed from json
--, Indexrn --removed from json
	, (select job_company_address as address
	, job_position as jobTitle
	, job_company_name as currentEmployer
	, convert(varchar(max),job_exp_years) as yearOfExperience
	, cb_Employer as cbEmployer
	, convert(varchar(10),job_from,120) as dateRangeFrom
	, convert(varchar(10),job_to,120) as dateRangeTo
	, concat_ws(char(10)
		, coalesce('Employer: ' + nullif(ltrim(rtrim(job_company_name)),''),NULL)
		, coalesce('Job Title: ' + nullif(ltrim(rtrim(job_position)),''),NULL)
		, coalesce('JobFunction: ' + nullif(ltrim(rtrim(JobFunction)),''),NULL)
		, coalesce('Job Industry: ' + nullif(ltrim(rtrim(JobIndustry)),''),NULL)
		, coalesce('What is the Company''s business?: ' + nullif(ltrim(rtrim(CompanyBusiness)),''),NULL)
		, coalesce('No. of Employees: ' + nullif(ltrim(rtrim(job_emp_number)),''),NULL)
		, coalesce('Employment From: ' + nullif(ltrim(rtrim(convert(varchar(10),job_from,120))),''),NULL)
		, coalesce('Employment To: ' + nullif(ltrim(rtrim(convert(varchar(10),job_to))),''),NULL)
		, coalesce('Position of direct boss: ' + nullif(ltrim(rtrim(DirectBoss)),''),NULL)
		, coalesce('Reason for leaving: ' + nullif(ltrim(rtrim(ReasonForLeaving)),''),NULL)
		, coalesce('Job Description: ' + nullif(ltrim(rtrim(JobDesc)),''),NULL)
		) as company
		from MainScript where cn_id = m.cn_id
		order by ReverseIndexrn asc
		for json path
		) as WorkHistory
from MainScript m --159256 rows (distinct to get 1 unique json for unique candidate)