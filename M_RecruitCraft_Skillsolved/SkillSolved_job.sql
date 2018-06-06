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
select concat('RC',v.contact_id) as 'position-contactId'
, concat('RC',v.vac_id) as 'position-externalId'
, iif(v.vac_id in (select vac_id from dup where dup.rn > 1)
	, iif(dup.vac_name = '' or dup.vac_name is NULL,concat('No job title - ',dup.vac_id),concat(dup.vac_name,' - ',dup.vac_id))
	, iif(v.vac_name = '' or v.vac_name is null,concat('No job title -',v.vac_id),v.vac_name)) as 'position-title'
, v.vac_name as OriginalVacName
, v.vac_no_positions as 'position-headcount'
, ltrim(rtrim(u.usr_email)) as 'position-owners'
, case
	when ltrim(rtrim(v.vac_type)) like '%Contract%' then 'CONTRACT'
	when ltrim(rtrim(v.vac_type)) like '%Full Time%' then 'PERMANENT'
	when ltrim(rtrim(v.vac_type)) like '%Permanent%' then 'PERMANENT'
	else 'PERMANENT' end as 'position-type'
, case
	when ltrim(rtrim(v.vac_type)) like '%Contract%' then 'CASUAL'
	when ltrim(rtrim(v.vac_type)) like '%Full Time%' then 'FULL_TIME'
	when ltrim(rtrim(v.vac_type)) like '%Permanent%' then 'FULL_TIME'
	else NULL end as 'position-employmentType'
, convert(varchar(10),v.vac_start_date,120) as 'position-startDate' -- no endDate as all jobs are required to be marked OPEN
, v.vac_salary as 'position-actualSalary'
, concat('RecruitCraft External ID: ',v.vac_id,char(10)
	, coalesce('Stage: ' + nullif(ltrim(rtrim(v.vac_stage)),'') + char(10),'')
	, coalesce('Company name: ' + nullif(ltrim(rtrim(v.company_name)),'') + char(10),'')
	, coalesce('Contact name: ' + nullif(ltrim(rtrim(v.contact_name)),'') + char(10),'')
	, coalesce('Vacancy fee: ' + nullif(ltrim(rtrim(v.vac_fee)),'') + char(10),'')) as 'position-note'
, vd.VacancyDocument as 'position-document'
, vc.VacancyComments as 'position-comment'
, nullif(cast(v.vac_desc_html as nvarchar(max)),'') as 'position-internalDescription'
from tblVacancies v
left join dup on dup.vac_id = v.vac_id
left join tblUser u on u.usr_id = v.consultant_id
left join VacancyDocument vd on vd.vacancy_id = v.vac_id
left join VacancyComments vc on vc.vacancy_id = v.vac_id
where v.vac_show = 1 and v.contact_id in (select contact_id from tblContacts where contact_show = 1)