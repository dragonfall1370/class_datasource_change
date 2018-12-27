--If contactID is empty or null, get max contactID
with ContactMaxID as (select distinct company_id, max(cn_id) as ContactMaxID
	from candidate.Candidates
	where can_type = 2
	and company_id in (select company_id from company.Companies)
	group by company_id)

--DUPLICATION REGCONITION
, dup as (select vac_id, vac_name, row_number() over(partition by lower(vac_name) order by vac_id desc) as rn
	from vacancies.Vacancies
	--where vac_show = 1 --|Filter removed per client request
	)

, NewJobTitle as (select vac_id
	, case when rn > 1 then concat(vac_name, ' - ', convert(varchar(max),vac_id))
	else ltrim(rtrim(vac_name)) end as NewJobTitle
	from dup)

--CONTACT DOCUMENTS (may include candidate documents)
, Documents as (select class_parent_id, doc_id, doc_class, doc_name, doc_blob_id, doc_ext 
	, case 
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) = 0 then doc_blob_id
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) > 0 then right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1)
		when charindex('.',doc_blob_id) = 0 and charindex('/',doc_blob_id) > 0 then concat(right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1),doc_ext)
		else concat(doc_blob_id,doc_ext) end as Documents
	from common.Documents
	where doc_class = 'Vacancy'
	and doc_ext <> ''
	)

, VacancyDocuments as (select class_parent_id
	, string_agg(convert(nvarchar(max),Documents),',') as VacancyDoc
	from Documents
	where class_parent_id > 0
	group by class_parent_id
	)

--JOB CONTACTS
, JobContact as (select v.vac_id, v.company_id
	, v.contact_id
	, case when (v.contact_id is NULL or not exists (select cn_id from candidate.Candidates where can_type = 2 and v.contact_id = cn_id)) 
		and exists (select company_id from ContactMaxID where v.company_id = company_id) then concat('PRTR',cm.ContactMaxID)
	  when v.contact_id in (select cn_id from candidate.Candidates where can_type = 2) then concat('PRTR',v.contact_id)
	  else 'PRTR999999999' end as NewContactID
	from vacancies.Vacancies v
	left join ContactMaxID cm on cm.company_id = v.company_id
	)

--MAIN SCRIPT
select concat('PRTR',v.vac_id) as 'position-externalId'
, jc.NewContactID as 'position-contactId'
, njt.NewJobTitle as 'position-title'
, case when v.vac_start_date is NULL and v.vac_timestamp is not NULL then v.vac_timestamp
	when v.vac_start_date is NULL and v.vac_timestamp is NULL then getdate()
	else v.vac_start_date end as 'position-startDate'
, case when v.vac_start_date is NULL and v.vac_timestamp < getdate() - 90 then getdate() - 7
	when v.vac_start_date is not NULL and v.vac_start_date < getdate() - 90 then getdate() - 7
	else NULL end as 'position-endDate' --CLOSE ALL JOBS where the START DATE is over 3 months from the database cut over date
, case when v.vac_currency in ('THB','BAht','BHT','BTH','THa') then 'THB'
	when v.vac_currency in ('BHD') then 'BHD'
	when v.vac_currency in ('TWD') then 'TWD'
	when v.vac_currency in ('USD') then 'USD'
	else 'THB' end as 'position-currency' --depending on RecruitCraft clients
	
/* Check if vac_salary_max is equal to vac_salary * 12 (months)
, case 
	when isnumeric(vac_salary_max) = 1 and isnumeric(vac_salary) = 1 then replace(left(vac_salary_max,len(vac_salary_max)-charindex('.',reverse(vac_salary_max))),',','')
	when isnumeric(vac_salary_max) <> 1 and isnumeric(vac_salary) = 1 then replace(left(vac_salary,len(vac_salary)-charindex('.',reverse(vac_salary))),',','')
	else NULL end as 'position-actualSalary'
*/

---CUSTOM SCRIPT #1
, case when isnumeric(v.vac_salary) = 1 then replace(left(vac_salary,len(vac_salary)-charindex('.',reverse(vac_salary))),',','')
	else NULL end as SalaryFrom
, case when isnumeric(v.vac_salary_max) = 1 then replace(left(vac_salary_max,len(vac_salary_max)-charindex('.',reverse(vac_salary_max))),',','')
	else NULL end as SalaryTo
---
, v.vac_no_positions as 'position-headcount'
, case when v.vac_type = 'Full Time' then 'PERMANENT'
	when v.vac_type = 'Part Time' then 'PERMANENT'
	when v.vac_type = 'Contract' then 'CONTRACT'
	when v.vac_type = 'Hiring' then 'PERMANENT'
	else NULL end as 'position-type'
, case when v.vac_type = 'Full Time' then 'FULL_TIME'
	when v.vac_type = 'Part Time' then 'PART_TIME'
	when v.vac_type = 'Contract' then 'CASUAL'
	when v.vac_type = 'Hiring' then 'FULL_TIME'
	else NULL end as 'position-employmentType'
, vd.VacancyDoc as 'position-document'
, u.usr_email as 'position-owners'
, v.vac_desc_html
, concat_ws(char(10)
	, coalesce('Company overview: ' + nullif(v.vac_benefits,''),NULL)
	, coalesce('Requirements: ' + nullif(case when v.vac_req like '%null%' then NULL else convert(nvarchar(max),v.vac_req) end,''),NULL)
	, coalesce('***Job Description: ' + char(10) + char(13) + nullif(convert(nvarchar(max),v.vac_desc_html),''),NULL)
	) as 'position-publicDescription'
, concat_ws(char(10), concat('Job external ID: ',v.vac_id)
	, coalesce('Generated ID: ' + nullif(ltrim(rtrim(v.vac_generated_id)),''),NULL)
	, coalesce('Stage: ' + nullif(ltrim(rtrim(v.vac_stage)),''),NULL)
	, coalesce('Publish Options: ' + nullif(ltrim(rtrim(v.vac_requested_by)),''),NULL)
	, coalesce('Nationality: ' + nullif(ltrim(rtrim(v.vac_nationality)),''),NULL)
	) as 'position-note'
from vacancies.Vacancies v
left join NewJobTitle njt on njt.vac_id = v.vac_id
left join users.Users u on u.usr_id = v.usr_id
left join VacancyDocuments vd on vd.class_parent_id = v.vac_id
left join JobContact jc on jc.vac_id = v.vac_id
order by v.vac_id