with
comploc as (
	select companyexternalId, companylocationAddress, companylocationCity,companylocationState,companylocationZipCode,companylocationCountry
	, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationAddress, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationZipCode, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCountry, ''), '')
			, 1, 1, '')) as 'locationAddress'
	, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			, 1, 1, '')) as 'locationName'
from company)

, dup as (SELECT companyexternalId,companyname,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(companyname)) ORDER BY companyexternalId DESC) AS rn 
	FROM company)

select 
	c.companyexternalId as 'company-externalId'
	--, companyname as 'company-name'
	, iif(c.companyexternalId in (select companyexternalId from dup where dup.rn > 1),concat(ltrim(rtrim(dup.companyname)),' ',dup.rn),ltrim(rtrim(c.companyname))) as 'company-name'
	, replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationAddress, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationZipCode, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCountry, ''), '')
			, 1, 1, '')),' ,',',') as 'company-locationAddress'
	, replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCountry, ''), '')
			, 1, 1, '')),' ,',',') as 'company-locationName'
	, companylocationCity as 'company-locationCity'
	, replace(companylocationCountry,'CAADA','CA') as 'company-locationCountry'
	, companylocationState as 'company-locationState'
	, companylocationZipCode as 'company-locationZipCode'
	--, companyphone1
	--, companyphone2
	--, companyphone3
	, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companyphone1, ''), '')
			+ Coalesce(',' + NULLIF(companyphone2, ''), '')
			+ Coalesce(',' + NULLIF(companyphone3, ''), '')
			, 1, 1, '')) as 'company-phone'
	, Stuff( Coalesce('Company External ID: '+ c.companyexternalId + char(10),'')
				+ Coalesce('Company Notes: ' + char(10) + nullif(companynote,'') + char(10), '')
                , 1, 0, '') as 'company-note'
	, companywebsite 'company-website'
from company c left join dup on c.companyexternalId = dup.companyexternalId
--where rn>1
--where companyexternalId = 'B0204'