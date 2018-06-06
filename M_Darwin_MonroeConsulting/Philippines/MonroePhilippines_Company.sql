with CompNote as (select n.ParentId, n.Text, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, u.Id
from Note n
left join Users u on n.CreatedBy = u.Id)

, CompanyNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT char(10) + 'Created by: ' + CreatedByName + ' || ' + Text
          from  CompNote
          WHERE ParentId = a.ParentId
		  order by Id desc
          FOR XML PATH (''))
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
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM DocumentEdited as a
where Filename not like '%.png%' and Filename not like '%.jpg%'
GROUP BY a.DynamicDataId)

select concat('MCPhilippines',C.DynamicDataId) as 'company-externalId'
, C.CompanyName as 'company-name'
, concat(COALESCE(C.Add1 + ', ',''),COALESCE(C.Add2 + ', ',''),COALESCE(C.Town + ', ',''),COALESCE(C.Postcode + ', ',''),COALESCE(C.Country,'')) as 'company-locationAddress'
, concat(COALESCE(C.Add1 + ', ',''),COALESCE(C.Add2 + ', ',''),COALESCE(C.Town + ', ',''),COALESCE(C.Postcode + ', ',''),COALESCE(C.Country,'')) as 'company-locationName'
, C.Town as 'company-locationCity'
, C.Country
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
, Postcode as 'company-locationZipCode'
, OfficeTelNo as 'company-phone'
, left(Website,99) as 'company-website' --current max char: 100
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
, iif(C.altbuilding = '' or C.altbuilding is NULL,'',concat('altbuilding: ',C.altbuilding,char(10)))
, iif(C.altadd1 = '' or C.altadd1 is NULL,'',concat('altAdd1: ',C.altadd1,char(10)))
, iif(C.altadd2 = '' or C.altadd2 is NULL,'',concat('altAdd2: ',C.altadd2,char(10)))
, iif(C.alttown = '' or C.alttown is NULL,'',concat('altTown: ',C.alttown,char(10)))
, iif(C.altcountry = '' or C.altcountry is NULL,'',concat('altCountry: ',C.altcountry,char(10)))
, iif(C.branchbuilding = '' or C.branchbuilding is NULL,'',concat('branchbuilding: ',C.branchbuilding,char(10)))
, iif(C.branchadd1 = '' or C.branchadd1 is NULL,'',concat('branchadd1: ',C.branchadd1,char(10)))
, iif(C.branchadd2 = '' or C.branchadd2 is NULL,'',concat('branchadd2: ',C.branchadd2,char(10)))
, iif(C.branchtown = '' or C.branchtown is NULL,'',concat('branchtown: ',C.branchtown,char(10)))
, iif(C.branchpostcode = '' or C.branchpostcode is NULL,'',concat('branchpostcode: ',C.branchpostcode,char(10)))
, iif(C.branchcountry = '' or C.branchcountry is NULL,'',concat('branchcountry: ',C.branchcountry,char(10)))
, iif(C.LastUpdatedDate = '' or C.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),C.LastUpdatedDate,120),char(10)))
, iif(C.LastUpdatedByEmail = '' or C.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',C.LastUpdatedByEmail,char(10)))
, replace(replace(replace(CN.Note,'&lt;','<'),'&gt;','>'),'&amp;','&')
),32000) as 'company-note'
, REPLACE(CF.ComFiles,'&amp;','&') as 'company-document'
from Company C
left join CompanyNote CN on C.DynamicDataId = CN.ParentId
left join CompanyFile CF on C.DynamicDataId = CF.DynamicDataId
order by C.DynamicDataId