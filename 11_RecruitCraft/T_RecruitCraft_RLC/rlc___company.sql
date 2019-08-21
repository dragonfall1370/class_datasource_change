/*
Company import requirement specs: 
https://hrboss.atlassian.net/wiki/spaces/SB/pages/19071426/Requirement+specs+Company+import

'company-externalId'
'company-name'
'company-locationAddress'
'company-locationName'
'company-locationCountry'
'company-locationState'
'company-locationCity'
'company-locationDistrict'
'company-locationZipCode'
'company-nearestTrainStation'
'company-headQuater'
'company-switchBoard'
'company-phone'
'company-fax'
'company-website'
'company-owners'
'company-document'
'company-note'
company-locationZipCode
 * ----------------------------------------------------------------------------------------------
 * don't add or map columns that require value is no in the field mapping
 * check if any contacts have no companies to referer to, then add a default company
 * replace concat/coalesce by concat_ws 
 * replace stuff by string_agg
 * ----------------------------------------------------------------------------------------------
 * Activities Comments: migrate by spoon
 * 
 */

--=======================================================================================================================================
--DUPLICATION RECOGNITION
with dup as (
SELECT 
	company_id, 
	iif(company_name='' or company_name is null, concat('Company name-', company_id), company_name) as company_name, 
	ROW_NUMBER() OVER(PARTITION BY company_name ORDER BY company_id ASC) AS rn 
FROM tblCompany where company_show = 1
)
--=======================================================================================================================================
--COMPANY DOCUMENTS
, Documents as (select company_id, doc_id, concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) as docfilename 
	from tblCompanyDocs)

, CompanyDocument as (
	SELECT
     	company_id,
		string_agg(cast(docfilename as nvarchar(max)), ', ') within group (order by doc_id desc) AS CompanyDocument
	FROM Documents as a
	GROUP BY a.company_id
)
--=======================================================================================================================================
--COMPANY NOTES
, CompanyComment as (
	SELECT 
		company_id,
		string_agg(cast(concat_ws(char(10)
		, 'Comment date: ' + convert(varchar(20),comment_date,120)
		, 'Consultant: ' + consultant
		, 'Comment: ' + comment) as nvarchar(max)), char(10))  WITHIN GROUP (order by comment_date desc) as cmd
	from tblCommentCompany group by company_id
)
--=======================================================================================================================================
--MAIN SCRIPT
select 
	concat('RLC',c.company_id) as 'company-externalId',
	iif(dup.rn > 1, concat(dup.company_name, dup.rn), dup.company_name) as 'company-name',
	
	concat_ws(', '
	, nullif(rtrim(ltrim(c.company_add1)),'')
	, nullif(rtrim(ltrim(c.company_add2)),'')
	, nullif(rtrim(ltrim(c.company_city)),'')
	, nullif(rtrim(ltrim(c.company_country)),'')
	, nullif(rtrim(ltrim(c.company_zip)),'')
	) as 'company-locationAddress',

	concat_ws(', '
	, nullif(rtrim(ltrim(c.company_city)),'')
	, nullif(rtrim(ltrim(c.company_country)),'')
	, nullif(rtrim(ltrim(c.company_zip)),'')
	) as 'company-locationName',	
	
	case 
--	 China
	when c.company_country = 'China' then 'CN'
	when c.company_country = 'PR of China' then 'CN'
	when c.company_country = 'CN' then 'CN'
--	 India
	when c.company_country = 'india' then 'IN'
	when c.company_country = 'IN' then 'IN'
--	 Malaysia
	when c.company_country = 'Malaysia' then 'MY'
	when c.company_country = 'MY' then 'MY'
--	 United State
	when c.company_country in ('USA', 'US') then 'US'
--	 HongKong
	when c.company_country = 'Hongkong' then 'HK'
	when c.company_country like ('%Hong Kong%') then 'HK'
--	 Turkey
	when c.company_country in ('Turkey') then 'TR'
--	 Tunisia
	when c.company_country in ('Tunisia') then 'TN'
	when c.company_country in ('Tunisia') then 'TN'
--	 United Kingdom
	when c.company_country in ('UK', 'United Kingdom', 'London', 'Derby') then 'GB'
--	 Taiwan
	when c.company_country in ('Taiwan') then 'TW'
--	 Ireland
	when c.company_country in ('Ireland') then 'IE'
--	 Poland
	when c.company_country in ('Poland') then 'PL'
--	 Netherlands
	when c.company_country in ('Netherlands', 'Holland') then 'NL'
--	 Philippines
	when c.company_country in ('Philippine', 'Philippines') then 'PH'
--	 Korea
	when c.company_country in ('Korea', 'Korea (South)') then 'KR'
--	 Singapore
	when c.company_country = 'Singapore' then 'SG'
	when c.company_country like '%Singapore%' then 'SG'
	when c.company_country in ('Thailand & Singapore') then 'SG'
--	 Germany 
	when c.company_country in ('Germany') then 'DE'
--	 France 
	when c.company_country in ('France') then 'FR'
--	 Estonia 
	when c.company_country in ('Estonia') then 'EE'
--	 Denmark 
	when c.company_country in ('Denmark') then 'DK'
--	 Canada  
	when c.company_country in ('Canada') then 'CA'
--	 Belgium  
	when c.company_country in ('Belgium') then 'BE'
--	 Austria   
	when c.company_country in ('Austria') then 'AT'
--	 Australia    
	when c.company_country in ('Australia') then 'AU'
--	 Sweden    
	when c.company_country in ('Sweden') then 'SE'
--	 Spain    
	when c.company_country in ('Spain') then 'ES'
--	 Thailand
	when c.company_country in (' Thailand','Bangkok','Thaialnd','Thailand','THAILAND','Thaniland, ','Thailand.','Thauland', 'Thailland', 'Thaniland') then 'TH'
	when c.company_country like '%Thailand%' then 'TH'
	when c.company_city like '%Bangkok%' then 'TH'
	when c.company_add2 like '%Bangkok%Thailand' then 'TH'
--	 exception cases needed to be reviewed
	else null end as 'company-locationCountry', -- check if any new countries and countries code

--'' as 'company-locationState', 	

	ltrim(rtrim(c.company_city)) as 'company-locationCity',

--'' as 'company-locationDistrict',

	ltrim(rtrim(c.company_zip)) as 'company-locationZipCode',

--'' as 'company-nearestTrainStation',
--'' as 'company-headQuarter', 
--'' as 'company-switchBoard',

	ltrim(rtrim(c.company_tel)) as 'company-phone', -- check if any company phone somewhere
--	ltrim(rtrim(c.company_fax)) as 'company-fax',
	left(ltrim(rtrim(c.company_web)),100) as 'company-website', -- why ltrim 100 chars (Vincere's data limitation)

	concat('External ID: ', c.company_id, char(10)
--	 , coalesce('Fax: ' + nullif(ltrim(rtrim(c.company_fax)),'') + char(10),'')
	, coalesce('Business sector: ' + nullif(ltrim(rtrim(c.company_sector)),'') + char(10),'')
--	, coalesce('Source: ' + nullif(ltrim(rtrim(c.company_source)),'') + char(10),'')
--	, coalesce('Stage: ' + nullif(ltrim(rtrim(c.company_stage)),'') + char(10),'')
	, coalesce('Status: ' + nullif(ltrim(rtrim(c.company_status)),'') + char(10),'')
--	, coalesce('No. of Employees: ' + nullif(ltrim(rtrim(c.company_num_emp)),'') + char(10),'')
--	, coalesce('Agreed Rate: ' + nullif(ltrim(rtrim(c.company_agreed_rate)),'') + char(10),'')
--	, coalesce('Other Rate: ' + nullif(ltrim(rtrim(c.company_other_rate)),'') + char(10),'')
--	 , Term Signed (radio button)
	, coalesce('Tax ID: ' + nullif(ltrim(rtrim(c.company_tax_id)),'') + char(10),'')
	
--	, coalesce('Company type: ' + nullif(ltrim(rtrim(c.company_type)),'') + char(10),'')
--	, coalesce('Other Rate: ' + nullif(ltrim(rtrim(c.company_other_rate)),'') + char(10),'')
--	, coalesce('Currency Code: ' + nullif(ltrim(rtrim(c.company_currency_code)),'') + char(10),'')
--	, coalesce('Tax ID: ' + nullif(ltrim(rtrim(c.company_tax_id)),'') + char(10),'')
--	, iif(c.company_sector_id = 0 or company_sector_id is NULL,'',coalesce('Industry: ' + nullif(ltrim(rtrim(ebc.JobExpBizCat)),''),''))
--	, iif(cc.CompanyComment is NULL,'',coalesce(char(10) + char(13) + nullif(ltrim(rtrim(cc.CompanyComment)),''),''))
	) as 'company-note'
	, ltrim(rtrim(u.usr_email)) as 'company-owners' -- map to consultant name
	, cd.CompanyDocument as 'company-document'
from tblCompany c
left join dup on dup.company_id = c.company_id
left join tblUser u on u.usr_id = c.consultant_id
left join tblLookUpJobExpBizCat ebc on ebc.JobExpBizCatID = c.company_sector_id
left join CompanyComment cc on cc.company_id = c.company_id
left join CompanyDocument cd on cd.company_id = c.company_id
where c.company_show = 1