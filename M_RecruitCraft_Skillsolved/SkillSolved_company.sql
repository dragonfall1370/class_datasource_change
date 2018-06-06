--DUPLICATION RECOGNITION
with dup as (SELECT company_id, company_name, ROW_NUMBER() OVER(PARTITION BY company_name ORDER BY company_id ASC) AS rn 
FROM tblCompany where company_show = 1)

--COMPANY DOCUMENTS
, Documents as (select company_id, doc_id, concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) as docfilename 
	from tblCompanyDocs)

, CompanyDocument as (SELECT
     company_id,
     STUFF(
         (SELECT ', ' + docfilename
          from  Documents
          WHERE company_id = a.company_id
		  order by doc_id desc --> order documents from the latest to oldest
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS CompanyDocument
FROM Documents as a
GROUP BY a.company_id)

--COMPANY NOTES
, CompanyComment as (SELECT
     company_id,
     STUFF(
         (SELECT char(10) + 'Comment date: ' + convert(varchar(20),comment_date,120) + char(10)
		 + 'Consultant: ' + consultant + char(10) + 'Comment: ' + comment
          from tblCommentCompany
          WHERE company_id = a.company_id
		  order by comment_date desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CompanyComment
FROM tblCommentCompany as a
GROUP BY a.company_id)


--MAIN SCRIPT
select concat('RC',c.company_id) as 'company-externalId'
, c.company_name as OriginalCompanyName
, iif(c.company_id in (select company_id from dup where dup.rn > 1)
	, iif(dup.company_name = '' or dup.company_name is NULL,concat('Company name -',dup.company_id),concat(dup.company_name,'-DUPLICATE-',dup.company_id))
	, iif(c.company_name = '' or c.company_name is null,concat('Company name -',dup.company_id),c.company_name)) as 'company-name'
, ltrim(rtrim(u.usr_email)) as 'company-owners'
, ltrim(rtrim(c.company_tel)) as 'company-phone'
, ltrim(rtrim(c.company_fax)) as 'company-fax'
, left(ltrim(rtrim(c.company_web)),100) as 'company-website'
, ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.company_add1)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.company_add2)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.company_city)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.company_zip)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.company_country)),''),'')),1,1,'')) as 'company-locationName'
, ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.company_add1)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.company_add2)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.company_city)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.company_zip)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.company_country)),''),'')),1,1,'')) as 'company-locationAddress'
, ltrim(rtrim(c.company_city)) as 'company-locationCity'
, ltrim(rtrim(c.company_zip)) as 'company-locationZipCode'
, c.company_country
, case 
	when c.company_country in (' Thailand','Bangkok','Thaialnd','Thailand','THAILAND','Thailand, ','Thailand.','Thauland') then 'TH'
	when c.company_country = 'China' then 'CN'
	when c.company_country = 'india' then 'IN'
	when c.company_country = 'Malaysia' then 'MY'
	when c.company_country = 'Singapore' then 'SG'
	when c.company_country = 'USA' then 'US'
	else NULL end as 'company-locationCountry'
, concat('RecruitCraft External ID: ',c.company_id,char(10)
	, coalesce('Company type: ' + nullif(ltrim(rtrim(c.company_type)),'') + char(10),'')
	, coalesce('Source: ' + nullif(ltrim(rtrim(c.company_source)),'') + char(10),'')
	, coalesce('Business sector: ' + nullif(ltrim(rtrim(c.company_sector)),'') + char(10),'')
	, coalesce('Stage: ' + nullif(ltrim(rtrim(c.company_stage)),'') + char(10),'')
	, coalesce('Status: ' + nullif(ltrim(rtrim(c.company_status)),'') + char(10),'')
	, coalesce('No. of Employees: ' + nullif(ltrim(rtrim(c.company_num_emp)),'') + char(10),'')
	, coalesce('Agreed Rate: ' + nullif(ltrim(rtrim(c.company_agreed_rate)),'') + char(10),'')
	--, coalesce('Other Rate: ' + nullif(ltrim(rtrim(c.company_other_rate)),'') + char(10),'')
	--, coalesce('Currency Code: ' + nullif(ltrim(rtrim(c.company_currency_code)),'') + char(10),'')
	--, coalesce('Tax ID: ' + nullif(ltrim(rtrim(c.company_tax_id)),'') + char(10),'')
	, iif(c.company_sector_id = 0 or company_sector_id is NULL,'',coalesce('Industry: ' + nullif(ltrim(rtrim(ebc.JobExpBizCat)),''),''))
	, iif(cc.CompanyComment is NULL,'',coalesce(char(10) + char(13) + nullif(ltrim(rtrim(cc.CompanyComment)),''),''))
	) as 'company-note'
, cd.CompanyDocument as 'company-document'
from tblCompany c
left join dup on dup.company_id = c.company_id
left join tblUser u on u.usr_id = c.consultant_id
left join tblLookUpJobExpBizCat ebc on ebc.JobExpBizCatID = c.company_sector_id
left join CompanyComment cc on cc.company_id = c.company_id
left join CompanyDocument cd on cd.company_id = c.company_id
where c.company_show = 1