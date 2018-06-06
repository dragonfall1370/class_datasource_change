with CompNote as (select n.ParentId, n.Text, n.CreatedDate, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, u.Id
from Notes n
left join Users u on n.CreatedBy = u.Id)

, CompanyNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT char(10) + 'Created date: ' + convert(varchar(20),CreatedDate,120) + char(10) + 'Created by: ' + CreatedByName + ' || ' + Text + char(10)
          from  CompNote
          WHERE ParentId = a.ParentId
		  order by CreatedDate desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM CompNote as a
GROUP BY a.ParentId)

, DocumentEdited as (select DynamicDataId, replace(Filename,',','') as Filename from Document)

, CompanyFile (DynamicDataId, ComFiles)
as (SELECT
     DynamicDataId, 
     STUFF(
         (SELECT ',' + Filename
          from  DocumentEdited
          WHERE DynamicDataId = a.DynamicDataId
		  and (Filename not like '%.png%' and Filename not like '%.jpg%')
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM DocumentEdited as a
where Filename not like '%.png%' and Filename not like '%.jpg%'
GROUP BY a.DynamicDataId)

--DUPLICATION REGCONITION
, dup as (SELECT DynamicDataId, CompanyName, ROW_NUMBER() OVER(PARTITION BY CompanyName ORDER BY DynamicDataId ASC) AS rn 
FROM Company)

--MAIN SCRIPT
select concat('MCThailand',C.DynamicDataId) as 'company-externalId'
, C.CompanyName as '(OriginalName)'
, iif(C.DynamicDataId in (select DynamicDataId from dup where dup.rn > 1)
	, iif(dup.CompanyName = '' or dup.CompanyName is NULL,concat('Default Company-',dup.DynamicDataId,' [TH]'),concat(dup.CompanyName,'-DUPLICATE-',dup.DynamicDataId,' [TH]'))
	, iif(C.CompanyName = '' or C.CompanyName is null,concat('Default Company-',dup.DynamicDataId,' [TH]'),concat(C.CompanyName,' [TH]'))) as 'company-name'
, ltrim(stuff((coalesce(', ' + C.Add1,'') + coalesce(', ' + C.Add2,'') + coalesce(',' + C.Town,'') 
	+ coalesce(', ' + C.Postcode,'') + coalesce(', ' + C.Country,'')),1,1,'')) as 'company-locationName'
, ltrim(stuff((coalesce(', ' + C.Add1,'') + coalesce(', ' + C.Add2,'') + coalesce(',' + C.Town,'') 
	+ coalesce(', ' + C.Postcode,'') + coalesce(', ' + C.Country,'')),1,1,'')) as 'company-locationAddress'
, C.Town as 'company-locationCity'
, C.Country as '(OriginalCountry)'
, case 
	when C.Country like '%Phi%' then 'PH'
	when C.Country = 'PH' then 'PH'
	when C.Country = 'Japan' then 'JP'
	when C.Country = 'Brunei ' then 'BN'
	when C.Country = 'China' then 'CN'
	when C.Country = 'Germany' then 'DE'
	when C.Country = 'India' then 'IN'
	when C.Country = 'Indonesia' then 'ID'
	when C.Country= 'Malaysia ' then 'MY'
	when C.Country = 'London' OR C.Country= 'United Kingdom' then 'GB'
	when C.Country= 'Madagascar' then 'MG'
	when C.Country = 'Malawi' then 'MW'
	when C.Country = 'Malaysia' then 'MY'
	when C.Country = 'Malaysian' then 'MY'
	when C.Country = 'Petaling Jaya, Selangor Darul Ehsan.' then 'MY'
	when C.Country = 'Saudi Arabia' then 'SA'
	when C.Country like '%Singapore%' then 'SG'
	when C.Country = 'Taiwan' then 'TW'
	when C.Country = 'Thailand ' then 'TH'
	when C.Country = 'United Arab Emirates' then 'AE'
	when C.Country = 'US' or C.Country = 'United States' or C.Country = 'USA' then 'US'
	when C.Country like '%Hong%' then 'HK'
	when C.Country = 'Spain' then 'ES'
	when C.Country = 'Vietnam' then 'VN'
	when C.Country = 'Australia' then 'AU'
	when C.Country = 'New Zealand' then 'NZ'
	when C.Country = 'Netherlands' then 'NL'
	when C.Country = 'Madagascar' then 'MG'
	when C.Country = 'Germany' then 'DE'
	when C.Country = 'India' then 'IN'
	when C.Country = 'Israel' then 'IL'
	when C.Country = 'Qatar' then 'QA'
	when C.Country = 'Poland' then 'PL'
	when C.Country = 'Canada' then 'CA'
	when C.Country = 'Afghanistan' then 'AF'
	when C.Country = 'Georgia' then 'GE'
	when C.Country = 'Oman' then 'OM'
	when C.Country = 'Turkey' then 'TR'
	when C.Country = 'Papua New Guinea' then 'PG'
	when C.Country = 'Denmark' then 'DK'
	when C.Country = 'Pakistan' then 'PK'
	when C.Country like '%Taiwan%' then 'TW'
	else '' end as 'company-locationCountry'
, C.Postcode as 'company-locationZipCode'
, OfficeTelNo as 'company-phone'
, left(C.Website,99) as 'company-website' --current max char: 100
, left(concat('Company External ID: ',C.DynamicDataId,char(10)
, iif(C.CreatedDate = '' or C.CreatedDate is NULL,'',concat('Created Date: ',C.CreatedDate,char(10)))
, iif(C.CompStatus = '' or C.CompStatus is NULL,'',concat('Status: ',C.CompStatus,char(10)))
, iif(C.Division = '' or C.Division is NULL,'',concat('Division: ',C.Division,char(10)))
, iif(C.Industry = '' or C.Industry is NULL,'',concat('Industry: ',C.Industry,char(10)))
, iif(C.Country = '' or C.Country is NULL,'',concat('Country: ',C.Country,char(10)))
, iif(C.NoofEmp = '' or C.NoofEmp is NULL,'',concat('No. of Employees: ',C.NoofEmp,char(10)))
, iif(C.AgreedRate = '' or C.AgreedRate is NULL,'',concat('Agreed Rate: ',C.AgreedRate,char(10)))
, iif(C.TermsSent = '' or C.TermsSent is NULL,'',concat('Terms Sent: ',C.TermsSent,char(10)))
, iif(C.TermsSigned = '' or C.TermsSigned is NULL,'',concat('Terms Signed: ',C.TermsSigned,char(10)))
, iif(C.AdditionalInfo = '' or C.AdditionalInfo is NULL,'',concat('Additional Info: ',C.AdditionalInfo,char(10)))
, iif(C.BuildingName = '' or C.BuildingName is NULL,'',concat('Building Name: ',C.BuildingName,char(10)))
, iif(C.CompanyRegNo = '' or C.CompanyRegNo is NULL,'',concat('Company Reg No: ',C.CompanyRegNo,char(10)))
, iif(C.FurtherInfo = '' or C.FurtherInfo is NULL,'',concat('Further Info: ',C.FurtherInfo,char(10)))
, iif(C.alt_building = '' or C.alt_building is NULL,'',concat('altbuilding: ',C.alt_building,char(10)))
, iif(C.altAdd1 = '' or C.altAdd1 is NULL,'',concat('altAdd1: ',C.altAdd1,char(10)))
, iif(C.altAdd2 = '' or C.altAdd2 is NULL,'',concat('altAdd2: ',C.altAdd2,char(10)))
, iif(C.altTown = '' or C.altTown is NULL,'',concat('altTown: ',C.altTown,char(10)))
, iif(C.altCountry = '' or C.altCountry is NULL,'',concat('altCountry: ',C.altCountry,char(10)))
, iif(C.br_Building = '' or C.br_Building is NULL,'',concat('Branch building: ',C.br_Building,char(10)))
, iif(C.br_add1 = '' or C.br_add1 is NULL,'',concat('br_add1: ',C.br_add1,char(10)))
, iif(C.br_add2 = '' or C.br_add2 is NULL,'',concat('br_add2: ',C.br_add2,char(10)))
, iif(C.br_town = '' or C.br_town is NULL,'',concat('br_town: ',C.br_town,char(10)))
, iif(C.br_postcode = '' or C.br_postcode is NULL,'',concat('br_postcode: ',C.br_postcode,char(10)))
, iif(C.br_country = '' or C.br_country is NULL,'',concat('br_country: ',C.br_country,char(10)))
, iif(C.LastUpdatedDate = '' or C.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),C.LastUpdatedDate,120),char(10)))
, iif(C.LastUpdatedByEmail = '' or C.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',C.LastUpdatedByEmail,char(10)))
, coalesce(char(10) + CN.Note,'')),32000) as 'company-note'
, CF.ComFiles as 'company-document'
from Company C
left join CompanyNote CN on C.DynamicDataId = CN.ParentId
left join CompanyFile CF on C.DynamicDataId = CF.DynamicDataId
left join dup on C.DynamicDataId = dup.DynamicDataId
order by C.DynamicDataId