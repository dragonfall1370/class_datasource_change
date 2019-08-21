with CompNote as (select n.ParentId, n.Text, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, n.CreatedDate 
from Note n
left join Users u on n.CreatedBy = u.Id)

, CompanyNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar(10),CreatedDate,120) + ' || ' + 'Created by: ' + CreatedByName + ' || ' + Text
          from  CompNote
          WHERE ParentId = a.ParentId
		  order by CreatedDate desc
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

select concat('MCMalaysia',C.DynamicDataId) as 'company-externalId'
, C.CompanyName as 'company-name'
, concat(COALESCE(C.Add1 + ', ',''),COALESCE(C.Add2 + ', ',''),COALESCE(C.Town + ', ',''),COALESCE(C.Postcode + ', ',''),COALESCE(C.Country,'')) as 'company-locationAddress'
, concat(COALESCE(C.Add1 + ', ',''),COALESCE(C.Add2 + ', ',''),COALESCE(C.Town + ', ',''),COALESCE(C.Postcode + ', ',''),COALESCE(C.Country,'')) as 'company-locationName'
, C.Town as 'company-locationCity'
, C.Country
, case C.Country 
when ' Indonesia' then 'ID'
when 'Australia' then 'AU'
when 'Brunei ' then 'BN'
when 'China' then 'CN'
when 'Germany' then 'DE'
when 'India' then 'IN'
when 'Indonesia' then 'ID'
when 'Johor ,Malaysia ' then 'MY'
when 'London' then 'GB'
when 'Madagascar' then 'MG'
when 'Malawi' then 'MW'
when 'Malaysia' then 'MY'
when 'Malaysian' then 'MY'
when 'Petaling Jaya, Selangor Darul Ehsan.' then 'MY'
when 'Philippines' then 'PH'
when 'Saudi Arabia' then 'SA'
when 'Singapore' then 'SG'
when 'Taiwan' then 'TW'
when 'Thailand ' then 'TH'
when 'United Arab Emirates' then 'AE'
else '' end as 'company-locationCountry'

, Postcode as 'company-locationZipCode'
, OfficeTelNo as 'company-phone'
, left(Website,99) as 'company-website' --current max char: 100
, left(concat('Company External ID: ',C.DynamicDataId,char(10)
, iif(C.CreatedDate = '' or C.CreatedDate is NULL,'',concat('Created Date: ',convert(varchar(10),C.CreatedDate,120),char(10)))
, iif(C.Status = '' or C.Status is NULL,'',concat('Status: ',C.Status,char(10)))
, iif(C.Division = '' or C.Division is NULL,'',concat('Division: ',C.Division,char(10)))
, iif(C.Industry = '' or C.Industry is NULL,'',concat('Industry: ',C.Industry,char(10)))
, iif(C.NoofEmp = '' or C.NoofEmp is NULL,'',concat('No. of Employees: ',C.NoofEmp,char(10)))
, iif(C.AgreedRate = '' or C.AgreedRate is NULL,'',concat('Agreed Rate: ',C.AgreedRate,char(10)))
, iif(C.TermsSent = '' or C.TermsSent is NULL,'',concat('Terms Sent: ',C.TermsSent,char(10)))
, iif(C.TermsSigned = '' or C.TermsSigned is NULL,'',concat('Terms Signed: ',C.TermsSigned,char(10)))
, iif(C.AdditionalInfo = '' or C.AdditionalInfo is NULL,'',concat('Additional Info: ',C.AdditionalInfo,char(10)))
, iif(C.BuildingName = '' or C.BuildingName is NULL,'',concat('Building Name: ',C.BuildingName,char(10)))
, iif(C.CompanyRegNo = '' or C.CompanyRegNo is NULL,'',concat('Company Reg No: ',C.CompanyRegNo,char(10)))
, iif(C.AccPayableContact = '' or C.AccPayableContact is NULL,'',concat('Acc Payable Contact: ',C.AccPayableContact,char(10)))
, iif(C.FurtherInfo = '' or C.FurtherInfo is NULL,'',concat('Further Info: ',C.FurtherInfo,char(10)))
, iif(C.alt_building = '' or C.alt_building is NULL,'',concat('alt_building: ',C.alt_building,char(10)))
, iif(C.altAdd1 = '' or C.altAdd1 is NULL,'',concat('altAdd1: ',C.altAdd1,char(10)))
, iif(C.altAdd2 = '' or C.altAdd2 is NULL,'',concat('altAdd2: ',C.altAdd2,char(10)))
, iif(C.altTown = '' or C.altTown is NULL,'',concat('altTown: ',C.altTown,char(10)))
, iif(C.altCountry = '' or C.altCountry is NULL,'',concat('altCountry: ',C.altCountry,char(10)))
, iif(C.altPostcode = '' or C.altPostcode is NULL,'',concat('altPostcode: ',C.altPostcode,char(10)))
, iif(C.LastUpdatedDate = '' or C.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),C.LastUpdatedDate,120),char(10)))
, iif(C.LastUpdatedByEmail = '' or C.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',C.LastUpdatedByEmail,char(10)))
, replace(replace(replace(CN.Note,'&lt;','<'),'&gt;','>'),'&amp;','&')
),32000) as 'company-note'
, REPLACE(CF.ComFiles,'&amp;','&') as 'company-document'
from Company C
left join CompanyNote CN on C.DynamicDataId = CN.ParentId
left join CompanyFile CF on C.DynamicDataId = CF.DynamicDataId
order by C.DynamicDataId