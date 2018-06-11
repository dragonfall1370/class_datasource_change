---DUPLICATION REGCONITION
with loc as (
	select ID, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(City, ''), '')
			+ Coalesce(', ' + NULLIF(Country, ''), '')
			, 1, 1, '')) as 'locationName'
	from company)

, dup as (SELECT ID, Companyname, ROW_NUMBER() OVER(PARTITION BY companyname ORDER BY ID ASC) AS rn 
FROM company)

----select * from dup
---Main Script---
select
  c.ID as 'company-externalId'
, C.Companyname as '(OriginalName)'
, iif(C.ID in (select ID from dup where dup.rn > 1)
	, iif(dup.Companyname = '' or dup.Companyname is NULL,concat('No Company Name - ',dup.ID),concat(dup.Companyname,' - ',dup.ID))
	, iif(C.Companyname = '' or C.Companyname is null,concat('No Company Name - ',C.ID),C.Companyname)) as 'company-name'
, iif(loc.locationName = '' or loc.locationName is NULL,'',ltrim(loc.locationName)) as 'company-locationName'
, iif(loc.locationName = '' or loc.locationName is NULL,'',ltrim(loc.locationName)) as 'company-locationAddress'
, iif(c.City = '','',c.City) as 'company-locationCity'
, case 
	when c.Country like '%Neder%' then 'NL'
	when c.Country like '%Duitsland%' then 'DE'
	when c.Country like '%Belgie%' then 'BE'
	when c.Country like '%UK%' then 'GB'
	else '' end as 'company-locationCountry'
, iif(Phone = '' or Phone is NULL,'',Phone) as 'company-phone'
, iif(Website like '%_.__%',Website,'') as 'company-website'
, left(Concat(
			'Company External ID: ', c.ID,char(10)
			, iif(c.IND = '' or c.IND is NULL,'',Concat(char(10), 'IND: ', c.IND, char(10)))
			, iif(Industry = '','',Concat(char(10), 'Industry: ', Industry, char(10)))
			, iif(c.Gebeld = '' or c.Gebeld is NULL,'',Concat(char(10), 'Gebeld: ', c.Gebeld, char(10)))
			, iif(c.Laatstecontact = '','',concat(char(10),'Laatste contact: ',c.Laatstecontact,char(10)))
			, iif(c.Opgenomen = '' or c.Opgenomen is NULL,'',Concat(char(10), 'Opgenomen: ', c.Opgenomen, char(10)))
			, iif(Gemaild = '','',Concat(char(10), 'Gemaild: ', Gemaild, char(10)))
			, iif(c.Opmerking = '','',concat(char(10),'Opmerking: ',char(10),c.Opmerking,char(10)))),32000)
			as 'company-note'
from company as c
			left join dup on c.ID = dup.ID
			left join loc on c.ID = loc.ID
where c.id = 'RECCLIENT08978'
--UNION ALL
--select 'REC9999999','','Default Company','','','','','','','This is Default Company from Data Import'

