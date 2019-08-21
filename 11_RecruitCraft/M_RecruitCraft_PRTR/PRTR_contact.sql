--CONTACT PRIMARY EMAIL
with 
SplitEmail as (select distinct cn_id
	, translate(value, '!'':"<>[]();,', '            ') as SplitEmail --to translate special characters
	from candidate.Candidates
	cross apply string_split(cn_cont_email,' ')
	where cn_cont_email like '%_@_%.__%'
	and can_type = 2 --CONTACT type
	)

, dup as (select cn_id
	, trim(' ' from SplitEmail) as EmailAddress
	, row_number() over(partition by trim(' ' from SplitEmail) order by cn_id asc) as rn --distinct email if emails exist more than once
	, row_number() over(partition by cn_id order by trim(' ' from SplitEmail)) as Contactrn --distinct if contacts may have more than 1 email
	from SplitEmail
	where SplitEmail like '%_@_%.__%'
	)

/* Check sample case
select * from dup
where cn_id = 5032928
*/

, PrimaryEmail as (select cn_id
	, case when rn > 1 then concat(rn,'_',EmailAddress)
	else EmailAddress end as PrimaryEmail
	from dup
	where EmailAddress is not NULL and EmailAddress <> ''
	and Contactrn = 1)

/* Check if more than 1 primary email after split
select cn_id from PrimaryEmail
group by cn_id having count(*) > 1
order by cn_id 
*/

--CONTACT PERSONAL EMAIL
, SplitEmail2 as (select distinct cn_id
	, translate(value, '!'':"<>[]();,', '            ') as SplitEmail2 --to translate special characters
	from candidate.Candidates
	cross apply string_split(cn_cont_email2,' ')
	where cn_cont_email2 like '%_@_%.__%'
	and can_type = 2 --CONTACT type
	)

, dup2 as (select cn_id
	, trim(' ' from SplitEmail2) as EmailAddress2
	, row_number() over(partition by trim(' ' from SplitEmail2) order by cn_id asc) as rn
	, row_number() over(partition by cn_id order by trim(' ' from SplitEmail2)) as Contactrn
	from SplitEmail2
	where SplitEmail2 like '%_@_%.__%'
	)

, PersonalEmail as (select cn_id
	, case when rn > 1 then concat(rn,'_',EmailAddress2)
	else EmailAddress2 end as PersonalEmail
	from dup2
	where EmailAddress2 is not NULL and EmailAddress2 <> ''
	and Contactrn = 1)

--CONTACT DOCUMENTS (may include candidate documents)
, Documents as (select class_parent_id, doc_id, doc_class, doc_name, doc_blob_id, doc_ext 
	, case 
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) = 0 then doc_blob_id
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) > 0 then right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1)
		when charindex('.',doc_blob_id) = 0 and charindex('/',doc_blob_id) > 0 then concat(right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1),doc_ext)
		else concat(doc_blob_id,doc_ext) end as Documents
	from common.Documents
	where doc_class = 'Candidate'
	and doc_ext <> '' and class_parent_id > 0
	)

, ContactDocuments as (select class_parent_id
	, string_agg(convert(nvarchar(max),Documents),',') as ContactDoc
	from Documents
	where class_parent_id > 0
	group by class_parent_id
	)

-->> MAIN SCRIPT <<--
select concat('PRTR',c.cn_id) as 'contact-externalId'
, case 
	when c.company_id is NULL or c.company_id not in (select company_id from company.Companies) then 'PRTR999999999'
	else concat('PRTR',c.company_id) end as 'contact-companyId'
, nullif(ltrim(rtrim(c.cn_fname)),'') as 'contact-firstName'
, coalesce(coalesce(nullif(c.cn_lname,''),nullif(c.cn_fname,'')),'Lastname') as 'contact-lastName' --Lastname is mandatory
--, concat_ws(' ',nullif(c.cn_fname,''),nullif(c.cn_lname,'')) as 'contact-lastName' --Lastname is mandatory
, coalesce(nullif(c.cn_fname_thai,''),'') as 'contact-firstNameKana'
, coalesce(nullif(c.cn_lname_thai,''),'') as 'contact-lastNameKana'
, case when c.cn_salut_text = 'Miss' then 'Miss.'
	when c.cn_salut_text = 'Mr.' then 'Mr.'
	when c.cn_salut_text = 'Mrs.' then 'Mrs.'
	when c.cn_salut_text = 'Ms.' then 'Ms.'
	else NULL end as ContactTitle --CUSTOM SCRIPT #1
, c.cn_present_position as 'contact-jobTitle'
, pe.PrimaryEmail as 'contact-email'
, prs.PersonalEmail as PersonalEmail --CUSTOM SCRIPT #2
, case when c.cn_cont_mobile in ('','N/A','null','NULL','-','x','xx','xxx') then NULL
	else ltrim(rtrim(c.cn_cont_mobile)) end as Mobile --CUSTOM SCRIPT #3
--, c.cn_skills as Nickname --CUSTOM SCRIPT #4 | Preferred Name
, concat_ws(','
		, case when c.cn_cont_bus in ('','N/A','null','NULL','-','x','xx','xxx') then NULL
			-- (case when c.cn_cont_mobile in ('','N/A','null','NULL','-','x','xx','xxx') then NULL
			-- else ltrim(rtrim(c.cn_cont_mobile)) end) --remove mobile in Primary to keep as is 20181106
			else concat(ltrim(rtrim(c.cn_cont_bus)),coalesce(' ext. ' + nullif(ltrim(rtrim(c.cn_car_license)),''),NULL)) end
		, case when c.cn_cont_other in ('','N/A','null','NULL','-','x','xx','xxx') then NULL
			else ltrim(rtrim(c.cn_cont_other)) end) as 'contact-phone'
, concat_ws(char(10), concat('Contact External ID: ',c.cn_id)
		, coalesce('Contact gender:' + case 
			when c.cn_sex = 1 then 'MALE'
			when c.cn_sex = 2 then 'FEMALE'
			else NULL end, NULL)
		, coalesce('Contact email 3: ' + nullif(ltrim(rtrim(c.cn_cont_email3)),''),NULL)
		, coalesce('Summary: ' + nullif(ltrim(rtrim(convert(nvarchar(max),c.cn_summary))),''),NULL)
		, coalesce('Comments: ' + nullif(ltrim(rtrim(convert(nvarchar(max),c.cn_comments))),''),NULL)
		) as 'contact-note'
, nullif(cd.ContactDoc,'') as 'contact-document'
from candidate.Candidates c
left join PrimaryEmail pe on pe.cn_id = c.cn_id
left join PersonalEmail prs on prs.cn_id = c.cn_id
left join ContactDocuments cd on cd.class_parent_id = c.cn_id
where c.can_type = 2 --62297

UNION ALL

select 'PRTR999999999','PRTR999999999','Default contact','','','','','','','','','This is default contact from Data Migration',''