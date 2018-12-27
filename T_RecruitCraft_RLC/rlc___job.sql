/* 
 * Requirement specs: Job import:
 * https://hrboss.atlassian.net/wiki/spaces/SB/pages/19071420/Requirement+specs+Job+import
 */
--'position-contactId'--1
--'position-title' --2  -must be unique
--'position-headcount'--3 --number-default value = 1
--'position-owners'--4
--'position-type'--5 PERMANENT, INTERIM_PROJECT_CONSULTING, TEMPORARY, CONTRACT. TEMPORARY_TO_PERMANENT--default PERMANENT
--'position-employmentType'--6 --FULL_TIME, PART_TIME, CASUAL --default FULL_TIME
--'position-comment'--7
--'position-currency'--8---http://www.currency-iso.org/en/home/tables/table-a1.html
--'position-actualSalary'--9
--'position-payRate'--10
--'position-contractLength'--11
--'position-publicDescription'--12
--'position-Description'--13
--'position-internalDescription'--14
--'position-externalId' --15
--'position-startDate'--16 yyyy-mm-dd
--'position-endDate'--17 yyyy-mm-dd
--'position-note' --18
--'position-document'--19
/*********************************** TODO: ON PRODUCTION SITE **********************************************/

--Migrate field Function (pull down) => Functional Expertise
 
/*********************************** TODO: ON PRODUCTION SITE **********************************************/
/*********************************** refer **********************************************/

--Select * From INFORMATION_SCHEMA.TABLES where TABLE_NAME like '%vacanc%'
--select * from XLVacancies; -- co industry

/*********************************** refer **********************************************/

--DUPLICATION REGCONITION
with dup as (SELECT vac_id, vac_name, ROW_NUMBER() OVER(PARTITION BY vac_name ORDER BY vac_id ASC) AS rn 
	FROM tblVacancies 
	where vac_show = 1 and contact_id in (select contact_id from tblContacts where contact_show = 1))

--VACANCY COMMENTS
, VacancyComments as (SELECT
     vacancy_id,
     STUFF(
         (SELECT char(10) + 'Comment date: ' + convert(varchar(20),comment_date,120) + char(10)
		 + 'Consultant: ' + ltrim(rtrim(consultant)) + char(10) + 'Comment: ' + comment
          from tblVacanciesComments
          WHERE vacancy_id = a.vacancy_id and vacancy_id is not NULL
		  order by comment_date desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS VacancyComments
FROM tblVacanciesComments as a
where a.vacancy_id is not NULL
GROUP BY a.vacancy_id)

--VACANCY DOCUMENTS
, Documents as (select vacancy_id, doc_id, concat(doc_id,'_',replace(replace(replace(doc_name,',',''),'.',''),'~$ ',''),rtrim(ltrim(doc_ext))) as docfilename 
	from tblVacanciesDocs)

, VacancyDocument as (SELECT
     vacancy_id,
     STUFF(
         (SELECT ', ' + docfilename
          from  Documents
          WHERE vacancy_id = a.vacancy_id
		  order by doc_id desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  as VacancyDocument
FROM Documents as a
GROUP BY a.vacancy_id)

--MAIN SCRIPT
select concat('RLC',v.contact_id) as 'position-contactId'
, concat('RLC',v.vac_id) as 'position-externalId'
, iif(v.vac_id in (select vac_id from dup where dup.rn > 1)
	, iif(dup.vac_name = '' or dup.vac_name is NULL,concat('No job title - ',dup.vac_id),concat(dup.vac_name,' - ',dup.vac_id))
	, iif(v.vac_name = '' or v.vac_name is null,concat('No job title -',v.vac_id),v.vac_name)) as 'position-title'
--, v.vac_name as OriginalVacName
--, v.vac_no_positions as 'position-headcount'
, ltrim(rtrim(u.usr_email)) as 'position-owners'
, case
	when ltrim(rtrim(v.vac_type)) like '%Contract%' then 'CONTRACT'
	when ltrim(rtrim(v.vac_type)) like '%Full time%' then 'PERMANENT'
	when ltrim(rtrim(v.vac_type)) like '%Permanent%' then 'PERMANENT'
	when ltrim(rtrim(v.vac_type)) like '%Part Time%' then 'TEMPORARY_TO_PERMANENT'
	else 'PERMANENT' end as 'position-type'
, case
	when ltrim(rtrim(v.vac_type)) like '%Contract%' then 'CASUAL'
	when ltrim(rtrim(v.vac_type)) like '%Full time%' then 'FULL_TIME'
	when ltrim(rtrim(v.vac_type)) like '%Permanent%' then 'FULL_TIME'
	when ltrim(rtrim(v.vac_type)) like '%Part Time%' then 'PART_TIME'
	else NULL end as 'position-employmentType'
	
, convert(varchar(10),v.vac_start_date,120) as 'position-startDate' 
, convert(varchar(10),v.vac_close_date,120) as 'position-endDate' 
--, cast(replace(v.vac_salary,',','') AS FLOAT) as 'position-actualSalary'
, case
	when v.vac_currency in ('THB', 'BHt', 'Thai Baht', 'thb') then 'THB'
	when v.vac_currency in ('EUR', 'EURO') then 'EURO'
	when v.vac_currency in ('USD', 'usd', 'US Dollar') then 'USD'
	when v.vac_currency in ('SGD', 'SINGD', 'Sing Dollar(SGD$)') then 'SINGD'
	when v.vac_currency in ('IDR', 'Indonesian Rupiah(IDRRp)') then 'IDR'
	when v.vac_currency in ('TJS', 'Somoni') then 'TJS'
	else null 
	end as 'position-currency' -- todo: remember to check currency again in the new projects
	
, concat('External ID: ', concat('RLC', v.vac_id),char(10)
	, coalesce('Stage: ' + nullif(ltrim(rtrim(v.vac_stage)),'') + char(10),'')
	, coalesce('Nationality: ' + nullif(ltrim(rtrim(v.vac_nationality)),'') + char(10),'')
	, coalesce('Hours: ' + nullif(ltrim(rtrim(v.vac_hours)),'') + char(10),'')
	, coalesce('Location: ' + nullif(ltrim(rtrim(v.vac_location)),'') + char(10),'')
	, coalesce('Vacancy fee: ' + nullif(ltrim(rtrim(v.vac_fee)),'') + char(10),'')
	, coalesce('Bonus: ' + nullif(ltrim(rtrim(v.vac_holidays)),'') + char(10),'')
	, coalesce('Benefits: ' + nullif(ltrim(rtrim(v.vac_benifits)),'') + char(10),'')
	, coalesce('Payment Cycle Type: ' + nullif(ltrim(rtrim(v.vac_pay_cycle)),'') + char(10),'')
--	, coalesce('Salary To: ' + nullif(ltrim(rtrim(v.vac_salary_max)),'') + char(10),'')
	, coalesce('Additional Notes: ' + nullif(ltrim(rtrim(v.vac_add_notes)),'') + char(10),'')
--	, coalesce('Company name: ' + nullif(ltrim(rtrim(v.company_name)),'') + char(10),'')
--	, coalesce('Contact name: ' + nullif(ltrim(rtrim(v.contact_name)),'') + char(10),'')
	) as 'position-note'

, concat(''
  , coalesce('Requirements: ' +char(10)+ '<br>' + nullif(ltrim(rtrim(cast(v.vac_req as nvarchar(max)))),'') + char(10),'')
	, coalesce('Experience: ' +char(10)+ '<br>' + nullif(ltrim(rtrim(cast(v.vac_exp as nvarchar(max)))),'') + char(10),'')
	, coalesce('Key Skills: ' +char(10)+ '<br>' + nullif(ltrim(rtrim(cast(v.vac_key_act as nvarchar(max)))),'') + char(10),'')
	, coalesce('Objectives: ' +char(10)+ '<br>' + nullif(ltrim(rtrim(cast(v.vac_obj as nvarchar(max)))),'') + char(10),'')
	) as 'position-publicDescription' 
	
, vd.VacancyDocument as 'position-document'
--, vc.VacancyComments as 'position-comment'
--, nullif(cast(v.vac_desc_html as nvarchar(max)),'') as 'position-internalDescription'
from tblVacancies v
left join dup on dup.vac_id = v.vac_id
left join tblUser u on u.usr_id = v.consultant_id
left join VacancyDocument vd on vd.vacancy_id = v.vac_id
left join VacancyComments vc on vc.vacancy_id = v.vac_id
where v.vac_show = 1 and v.contact_id in (select contact_id from tblContacts where contact_show = 1)
