--DUPLICATION RECOGNITION
with dup as (SELECT company_id, company_name, ROW_NUMBER() OVER(PARTITION BY lower(ltrim(company_name)) ORDER BY company_id ASC) AS rn 
FROM company.Companies 
--where company_show = 1
)

--COMPANY OWNERS (consultant split)
, CompanyOwner as (select company_id, company_owner_ids
	, value as company_owner
	from company.Companies c
	cross apply string_split(company_owner_ids, ',')
	)

--Company may have duplicate owners
, DistinctCompOwner as (select distinct co.company_id
	, co.company_owner
	, u.usr_email
	from CompanyOwner co
	left join Users.Users u on u.usr_id = co.company_owner)

, CompanyOwnerFinal as (select company_id
	, string_agg(convert(nvarchar(max),trim(' ' from nullif(usr_email,''))),',') as CompanyOwnerFinal
	from DistinctCompOwner
	group by company_id)

--COMPANY INDUSTRY: injected as Company Industry | CUSTOM SCRIPT #1 | Will use Candidate Industries as correct one
, CompanyIndustry as (select c.company_id, c.company_sector_id
	, l.listvalue
	from company.Companies c
	left join common.Lists l on c.company_sector_id = l.k_id
	where l.listkey = 'tblCompanyIndustries'
	)

--TO BE CONFIRMED: Company & Candidate use different set of Industries. To confirm to use 1 unique set of Industries	
, CompIndustry2 as (select c.company_id, c.company_sector_id
	, l.listvalue
	from company.Companies c
	left join common.Lists l on c.company_sector_id = l.k_id
	where l.listkey = 'tblLookupJobExpBizCat'
	)

--COMPANY DOCUMENTS
, Documents as (select class_parent_id, doc_id, doc_class, doc_name, doc_blob_id, doc_ext 
	, case 
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) = 0 then doc_blob_id
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) > 0 then right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1)
		when charindex('.',doc_blob_id) = 0 and charindex('/',doc_blob_id) > 0 then concat(right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1),doc_ext)
		else concat(doc_blob_id,doc_ext) end as Documents
	from common.Documents
	where doc_class = 'Company'
	and doc_ext <> ''
	)

, CompanyDocuments as (select class_parent_id
	, string_agg(convert(nvarchar(max),Documents),',') as CompanyDoc
	from Documents
	where class_parent_id > 0
	group by class_parent_id
	)

--COMPANY ADDRESS
, CompanyAddress as (select c.company_id, company_add1, company_add2, company_city, company_zip, company_country, add_default, company_tax_ID
	, case 
	when c.company_country = 'Antigua and Barbuda' then 'AG'
	when c.company_country = 'Siggapore' then 'SG'
	when c.company_country = 'Finland' then 'FI'
	when c.company_country = 'U.S.A' then 'US'
	when c.company_country = 'Thailnd' then 'TH'
	when c.company_country = 'South Korea' then 'KR'
	when c.company_country = 'Thialand' then 'TH'
	when c.company_country = 'Thailang' then 'TH'
	when c.company_country = 'Vietnam' then 'VN'
	when c.company_country = 'USA' then 'US'
	when c.company_country = 'Thailnad' then 'TH'
	when c.company_country = 'Angola' then 'AO'
	when c.company_country = 'Tahiland' then 'TH'
	when c.company_country = 'Italy' then 'IT'
	when c.company_country = 'TH1' then 'TH'
	when c.company_country = 'Netherlands' then 'NL'
	when c.company_country = 'Hong Kong, ' then 'HK'
	when c.company_country = 'Hong Kong' then 'HK'
	when c.company_country = 'Malaysia' then 'MY'
	when c.company_country = ' THAILAND' then 'TH'
	when c.company_country = 'Indonesia' then 'ID'
	when c.company_country = 'Thailand.' then 'TH'
	when c.company_country = 'Germany' then 'DE'
	when c.company_country = 'Philippines' then 'PH'
	when c.company_country = 'England' then 'GB'
	when c.company_country = 'Lithuania' then 'LT'
	when c.company_country = 'Singapore (SEA HQ)' then 'SG'
	when c.company_country = 'Thaniland' then 'TH'
	when c.company_country = 'Singapore 609914' then 'SG'
	when c.company_country = 'Thailans' then 'TH'
	when c.company_country = 'SAMUTPRAKARN  THAILAND.' then 'TH'
	when c.company_country = 'Switzerland' then 'CH'
	when c.company_country = 'United States' then 'US'
	when c.company_country = 'Korea' then 'KR'
	when c.company_country = 'The People’s Republic of China' then 'CN'
	when c.company_country = 'Thai' then 'TH'
	when c.company_country = 'Hongkong ' then 'HK'
	when c.company_country = ' PRC ' then 'CN'
	when c.company_country = 'Falkland Islands (UK)' then 'GB'
	when c.company_country = 'Laos' then 'LA'
	when c.company_country = 'Pathumwan, Bangkok' then 'TH'
	when c.company_country = 'Thaiiland' then 'TH'
	when c.company_country = 'Australia' then 'AU'
	when c.company_country = 'The Netheland ' then 'NL'
	when c.company_country = 'Bangkok, Thailand' then 'TH'
	when c.company_country = ' China' then 'CN'
	when c.company_country = 'Thaialnd' then 'TH'
	when c.company_country = 'Thailland' then 'TH'
	when c.company_country = 'Anguilla (UK)' then 'GB'
	when c.company_country = 'Bangkok 10250' then 'TH'
	when c.company_country = 'United Kingdom' then 'GB'
	when c.company_country = 'Mexico' then 'MX'
	when c.company_country = 'HK' then 'HK'
	when c.company_country = 'Albania' then 'AL'
	when c.company_country = 'Myanmar' then 'MM'
	when c.company_country = 'Khlongtoey Nue, Wattana, Bangkok 10110' then 'TH'
	when c.company_country = 'Sweden' then 'SE'
	when c.company_country = 'Netherland' then 'NL'
	when c.company_country = 'Thaliand' then 'TH'
	when c.company_country = 'Thailanad' then 'TH'
	when c.company_country = 'Viet Nam ' then 'VN'
	when c.company_country = ' Indonesia' then 'ID'
	when c.company_country = 'Wattana, Bangkok 10110, Thailand ' then 'TH'
	when c.company_country = 'Britain, Australia' then 'AU'
	when c.company_country = 'Singapre' then 'SG'
	when c.company_country = 'China' then 'CN'
	when c.company_country = 'Argentina' then 'AR'
	when c.company_country = 'People''s Republic of China ' then 'CN'
	when c.company_country = 'CA' then 'CA'
	when c.company_country = 'Armenia' then 'AM'
	when c.company_country = 'PR. China, Hongkong' then 'HK'
	when c.company_country = 'German' then 'DE'
	when c.company_country = '21140 Thailand' then 'TH'
	when c.company_country = 'Bangna, Bangkok 10260' then 'TH'
	when c.company_country = 'Bangkok 10110 Thailand' then 'TH'
	when c.company_country = 'India' then 'IN'
	when c.company_country = 'Hong Kong (China)' then 'HK'
	when c.company_country = 'Austria' then 'AT'
	when c.company_country = 'Greece' then 'GR'
	when c.company_country = 'United Arab Emirates' then 'AE'
	when c.company_country = 'Samutprakarn 10270, Thailand.' then 'TH'
	when c.company_country = 'UK' then 'GB'
	when c.company_country = 'Israel' then 'IL'
	when c.company_country = 'Bangrak Bangkok 10500' then 'TH'
	when c.company_country = 'Kuala Lumpur' then 'ML'
	when c.company_country = 'Thailandd' then 'TH'
	when c.company_country = 'West Malaysia' then 'ML'
	when c.company_country = 'Poland' then 'PL'
	when c.company_country = 'Canada' then 'CA'
	when c.company_country = 'TH' then 'TH'
	when c.company_country = 'Afghanistan' then 'AF'
	when c.company_country = ' Australia ' then 'AU'
	when c.company_country = 'Aruba (Netherlands)' then 'NL'
	when c.company_country = 'Wattana, Bangkok 10110 Thailand' then 'TH'
	when c.company_country = 'IRELAND' then 'IE'
	when c.company_country = 'Wantonglang, Bangkok 10110, Thailand' then 'TH'
	when c.company_country = 'U.S.A.' then 'US'
	when c.company_country = 'Thnailand' then 'TH'
	when c.company_country = 'Bangkok 10110' then 'TH'
	when c.company_country = 'Thailand' then 'TH'
	when c.company_country = 'Norway' then 'NO'
	when c.company_country = 'France' then 'FR'
	when c.company_country = 'THE NETHERLANDS' then 'NL'
	when c.company_country = ' Singapore' then 'SG'
	when c.company_country = 'Belgium' then 'BE'
	when c.company_country = 'Japan' then 'JP'
	when c.company_country = 'Thaoland' then 'TH'
	when c.company_country = 'Thaland' then 'TH'
	when c.company_country = 'Thailand,' then 'TH'
	when c.company_country = 'Singpapore' then 'SG'
	when c.company_country = 'Spain' then 'ES'
	when c.company_country = 'Thaiand' then 'TH'
	when c.company_country = ' India ' then 'IN'
	when c.company_country = 'Turkey' then 'TR'
	when c.company_country = '  Australia' then 'AU'
	when c.company_country = 'Thailamd' then 'TH'
	when c.company_country = 'Bangladesh' then 'BD'
	when c.company_country = 'Denmark' then 'DK'
	when c.company_country = 'Pakistan' then 'PK'
	when c.company_country = 'Singapore' then 'SG'
	when c.company_country = 'Thailamnd' then 'TH'
	when c.company_country = 'Cambodia' then 'KH'
	when c.company_country = 'singapore (head office)' then 'SG'
	when c.company_country = 'HQ - UK' then 'GB'
	when c.company_country = 'bangkok Thailand 10600' then 'TH'
	when c.company_country = 'Czech Republic' then 'CZ'
	when c.company_country = 'Bangkok 10330' then 'TH'
	when c.company_country = 'Taiwan' then 'TW'
	when c.company_country = 'Italian' then 'IT'
	when c.company_country = 'Phuket' then 'TH'
	when c.company_country = 'SG' then 'SG'
	when c.company_country = 'Thailan d' then 'TH'
	else NULL end as countryCode
	, row_number() over (partition by company_id order by address_id desc) as rn
	from company.Addresses c
	)

, CompanyAddress1 as (select company_id
	, trim(' ' from company_city) as company_city
	, trim(' ' from company_zip) as company_zip
	, ltrim(rtrim(company_country)) as company_country
	, countryCode
	, concat_ws(', ', nullif(company_add1,'')
		, nullif(company_add2,'')
		, nullif(ltrim(rtrim(company_city)),'')
		, nullif(trim(' ' from company_zip),'')
		, nullif(company_country,'')) as locationAddress
	, concat_ws(', '
		, nullif(trim(' ' from company_city),'')
		, nullif(trim(' ' from company_zip),'')
		, nullif(ltrim(rtrim(company_country)),'')) as locationName
	from CompanyAddress
	where rn = 1
	)

, companyMultiTax as (select company_id
	, string_agg (nullif(company_tax_ID,''),', ') as companyMultiTax
	from CompanyAddress
	group by company_id
	)

--To be injected as CUSTOM SCRIPT #2
, companyBussinessNo as (select company_id, company_tax_ID
	from CompanyAddress
	where rn = 1
	and company_tax_ID is not NULL and company_tax_ID not in ('','x','xxx','-','NA','n/a','tbc')
	)

--MAIN SCRIPT
select concat('PRTR',c.company_id) as 'company-externalId'
, iif(dup.rn > 1, concat(dup.company_name, ' - ', dup.rn), c.company_name) as 'company-name'
, ca.locationName as 'company-locationName'
, ca.locationAddress as 'company-locationAddress'
, ca.company_city as 'company-locationCity'
, ca.company_zip as 'company-locationZipCode'
, ca.countryCode as 'company-locationCountry'
, co.CompanyOwnerFinal as 'company-owners'
, left(c.company_web,100) as 'company-website'
, c.company_tel as 'company-phone'
--, ca.company_add1 as HeadQuarters --CUSTOM SCRIPT #5: added on 20181101
--, c.company_num_emp --CUSTOM SCRIPT #3
--, c.company_sector --CUSTOM SCRIPT #4 What is the company business ?
, concat_ws(char(10),concat('Company external ID: ',c.company_id)
	, coalesce('What is the company business ?: ' + nullif(c.company_sector,''),NULL) --updated on 20181101
	, coalesce('Telephone 2: ' + nullif(json_value(c.company_custom_field,'$.SubDetail.dxTextBox[0].Value'),''),NULL) --get value from json
	, coalesce('Telephone 3: ' + nullif(json_value(c.company_custom_field,'$.SubDetail.dxTextBox[1].Value'),''),NULL)
	-- , coalesce('Company Type: ' + nullif(c.company_type,''),NULL) --requirements changed on 20181101
	-- , coalesce('Business Numbers (TaxID): ' + nullif(ct.companyMultiTax,''),NULL) --requirements changed on 20181101
	-- , coalesce('Agreed Rate: ' + nullif(c.company_agreed_rate,''),NULL) --requirements changed on 20181101
	-- , coalesce('Additional Info: ' + nullif(c.company_add_info,''),NULL) --requirements changed on 20181101
	) as 'company-note'
, cd.CompanyDoc as 'company-document'
from company.Companies c
left join dup on dup.company_id = c.company_id
left join CompanyAddress1 ca on ca.company_id = c.company_id
left join companyMultiTax ct on ct.company_id = c.company_id
left join CompanyOwnerFinal co on co.company_id = c.company_id
left join CompanyDocuments cd on cd.class_parent_id = c.company_id
--where c.company_show = 1 --No filter on REVIEW site

UNION ALL

select 'PRTR999999999','Default company','','','','','','','','','This is default company from data migration',''