--SPECIAL NOTES:
/* In Company, there are some duplicated rows that should be removed before processing */

--DUPLICATION REGCONITION
with dup as (select id, name, row_number() over(partition by lower(name) order by id asc) as rn
	from companies)

, CompanyName as (select distinct id, name,
	case when rn > 1 then concat(name,' - ', id)
	else name end as CompanyName
	from dup)

--COMPANY FINAL DOCUMENTS (after creating companyDocument table)
, Documents as (select data_item_id, string_agg(cast(concat(id, right(filename, charindex('.',reverse(filename)))) as nvarchar(max)),',') as companyDocuments
	from attachments
	where data_item_type = 'company'
	group by data_item_id)
	
--COMPANY ADDRESS
, CompanyAddress as (select id, concat_ws(', ', nullif(addressstreet,'')
	,nullif(nullif(addresscity,''),'NA'),nullif(nullif(addressstate,''),'NA'),nullif(nullif(addresspostalcode,''),'NA')
	,nullif(nullif(countrycode,''),'NA')) as CompanyAddress
	from companies)

--COMPANY INDUSTRY (after creating companyIndustry table)

/*
--COMPANY DEPARTMENT 
, Department as (select id, embeddeddepartments0id as DepartmentID, embeddeddepartments0name as DepartmentName from company UNION ALL
select id, embeddeddepartments1id, embeddeddepartments1name from company UNION ALL
select id, embeddeddepartments2id, embeddeddepartments2name from company UNION ALL
select id, embeddeddepartments3id, embeddeddepartments3name from company UNION ALL
select id, embeddeddepartments4id, embeddeddepartments4name from company UNION ALL
select id, embeddeddepartments5id, embeddeddepartments5name from company UNION ALL
select id, embeddeddepartments6id, embeddeddepartments6name from company UNION ALL
select id, embeddeddepartments7id, embeddeddepartments7name from company UNION ALL
select id, embeddeddepartments8id, embeddeddepartments8name from company UNION ALL
select id, embeddeddepartments9id, embeddeddepartments9name from company UNION ALL
select id, embeddeddepartments10id, embeddeddepartments10name from company UNION ALL
select id, embeddeddepartments11id, embeddeddepartments11name from company UNION ALL
select id, embeddeddepartments12id, embeddeddepartments12name from company UNION ALL
select id, embeddeddepartments13id, embeddeddepartments13name from company)

, companyDepartment as (select id, string_agg(DepartmentName,', ') as companyDepartment
	from Department
	where DepartmentName <> 'NA' and DepartmentName <> ''
	group by id)
*/
, companyDepartment as (select companies_id
	, string_agg(nullif(name,''),', ') as companyDepartment
	from companies_departments
	group by companies_id)

/*
--COMPANY PHONES
, Phones as (select id
	, concat(case when embeddedphones0number in ('NA','') or embeddedphones0number is NULL then NULL else embeddedphones0number end
		, case when embeddedphones0extension in ('NA','') or embeddedphones0extension is NULL then NULL else ' ext.' || embeddedphones0extension end) as phone1
	, concat(case when embeddedphones1number in ('NA','') or embeddedphones1number is NULL then NULL else embeddedphones1number end
		, case when embeddedphones1extension in ('NA','') or embeddedphones1extension is NULL then NULL else ' ext.' || embeddedphones1extension end) as phone2
	from companies
)

, FinalPhone as (select id
	, case when phone1 = 'NA' then '' else coalesce(phone1,'') end as phone1
	, case when phone2 = 'NA' then '' else coalesce(phone2,'') end as phone2
	from Phones)

, companyPhone as (select id
	, case when phone1 = '' and phone2 = '' then NULL
		when phone1 = '' then phone2 
		when phone2 = '' then phone1
		else concat_ws(', ',phone1,phone2) end as companyPhone
	from FinalPhone)
*/

, FeeAgreement as (select cv.id
		, cf.name
		, cf_value
		from companies_custom_fields_value cv
		left join companies_custom_fields cf on cf.id = cv.cf_id
		where cf_value is not NULL
		and cf_id = 175996) --Fee Agreement %

--MAIN SCRIPT
select concat('CG',c.id) as [company-externalId]
, cn.CompanyName as [company-name]
, ca.CompanyAddress as [company-locationName]
, ca.CompanyAddress as [company-locationAddress]
, nullif(c.addresscity,'NA') as [company-locationCity]
, nullif(c.addressstate,'NA') as [company-locationState]
, nullif(c.addresspostalcode,'NA') as [company-locationZipCode]
, case when c.countrycode = 'NA' or c.countrycode = '' or c.countrycode is NULL then NULL
	when c.countrycode in ('CD','AN','SS') then NULL
	else c.countrycode end as [company-locationCountry]
, left(c.website,100) as [company-website]
, case when c.ownerid = '0' then NULL
	else trim(u.username) end as [company-owners]
, concat_ws(', ', nullif(c.phonesprimary,''), nullif(c.phonessecondary,'')) as [company-phone]
, d.companyDocuments as [company-document]
, c.datecreated --CUSTOM SCRIPT #1 Reg Date
, concat_ws(char(10)
	, concat('Company external ID: ', c.id)
	, coalesce('Registation Date: ' + convert(nvarchar(20),c.datecreated,120), NULL)
	, coalesce('Date modified: ' + convert(nvarchar(20),c.datemodified,120), NULL)
	, case when c.embeddedstatustitle <> '' then concat('Status: ', c.embeddedstatustitle) else NULL end
	, coalesce('Status date: ' + convert(nvarchar(20),c.datemodified,120), NULL)
	, coalesce('Fee Agreement %: ' + nullif(fa.cf_value,''),NULL)
	, coalesce('Departments: ' + nullif(cd.companyDepartment,''), NULL)
	, coalesce('*** Notes: ' + nullif(convert(nvarchar(max),c.notes),''),NULL)
	) as [company-note]
from companies c
left join CompanyName cn on cn.id = c.id
left join CompanyAddress ca on ca.id = c.id
left join FeeAgreement fa on fa.id = c.id
left join companyDepartment cd on cd.companies_id = c.id
left join Documents d on d.data_item_id = c.id
--left join companyPhone cp on cp.id = c.id
left join users u on u.id = c.ownerid --total: 11300

UNION ALL

select 'CG999999999','Default company','','','','','','','','','','','','This is default company from data migration'