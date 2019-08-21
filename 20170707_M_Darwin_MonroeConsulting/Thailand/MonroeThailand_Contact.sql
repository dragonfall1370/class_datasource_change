with ConNote as (select n.ParentId, n.Text, n.CreatedDate, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, u.Id 
from Notes n
left join Users u on n.CreatedBy = u.Id)

, ContactNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT '<hr>' + 'Created date: ' + convert(varchar(20),CreatedDate,120) + char(10) + 'Created by: ' + CreatedByName + ' || ' + Text
          from  ConNote
          WHERE ParentId = a.ParentId
		  order by CreatedDate desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 4, '')  AS URLList
FROM ConNote as a
GROUP BY a.ParentId)

, DocumentEdited as (select DynamicDataId, replace(Filename,',','') as Filename from Document)

, ContactFile (DynamicDataId, ConFiles)
as (SELECT
     DynamicDataId, 
     STUFF(
         (SELECT ',' + Filename
          from  DocumentEdited
          WHERE DynamicDataId = a.DynamicDataId
		  and (Filename not like '%.png%' and Filename not like '%.jpg%')
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM DocumentEdited as a
where Filename not like '%.png%' and Filename not like '%.jpg%'
GROUP BY a.DynamicDataId)

--CONTACT DUPLICATE MAIL RECOGNITION
, CombinedEmail as (
select DynamicDataId, Email as ContactEmail from Contact where Email is not NULL and Email like '%_@_%.__%'
UNION ALL
select DynamicDataId, Email2 from Contact where Email2 is not NULL and Email2 like '%_@_%.__%')

, EmailDupRegconition as (SELECT distinct DynamicDataId, ContactEmail, ROW_NUMBER() OVER(PARTITION BY ContactEmail ORDER BY DynamicDataId ASC) AS rn 
from CombinedEmail)

, ContactEmail as (select DynamicDataId
, case	when rn = 1 then ContactEmail
		else concat(rn,'-',ContactEmail) end as ContactEmail
, rn
from EmailDupRegconition)

, ContactEmailFinal as (SELECT
     DynamicDataId,
     STUFF(
         (SELECT ',' + ContactEmail
          from  ContactEmail
          WHERE DynamicDataId = a.DynamicDataId
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 1, '')  AS ContactEmailFinal
FROM ContactEmail as a
GROUP BY a.DynamicDataId)

--MAIN SCRIPT
select 
iif(C.CompanyID = '' or C.CompanyID is NULL,'MCThailand9999999',concat('MCThailand',C.CompanyID)) as 'contact-companyId'
, C.CompanyID as '(OriginalCompanyID)'
, C.Company as '(OriginalCompanyName)'
, concat('MCThailand',C.DynamicDataId) as 'contact-externalId'
, iif(C.FirstName = '' or C.FirstName is NULL,'Firstname',C.FirstName) as 'contact-firstName'
, iif(C.LastName = '' or C.LastName is NULL,concat('LastName-',C.DynamicDataId),C.LastName) as 'contact-lastName'
, CEF.ContactEmailFinal as 'contact-email'
, stuff((coalesce(', ' + C.Tel,'') + coalesce(', ' + C.MobileCon,'')),1,1,'') as 'contact-phone'
, C.Position as 'contact-jobTitle'
, concat('Contact External ID: ',C.DynamicDataId,char(10)
	, iif(C.Title = '' or C.Title is NULL,'',concat('Candidate title: ',C.Title,120,char(10)))
	, iif(C.CreatedDate = '' or C.CreatedDate is NULL,'',concat('Created Date: ',C.CreatedDate,120,char(10)))
	, iif(C.Name = '' or C.Name is NULL,'',concat('Contact Name: ',C.Name,char(10)))
	, iif(C.Fax = '' or C.Fax is NULL,'',concat('Fax: ',C.Fax,char(10)))
	, iif(C.Extn = '' or C.Extn is NULL,'',concat('Extension: ',C.Extn,char(10)))
	, iif(C.Division = '' or C.Division is NULL,'',concat('Division: ',C.Division,char(10)))
	, iif(C.knownAs = '' or C.knownAs is NULL,'',concat('Known as: ',C.knownAs,char(10)))
	, iif(C.Company = '' or C.Company is NULL,'',concat('Company name: ',C.Company,char(10)))
	, iif(C.LastUpdatedDate = '' or C.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),C.LastUpdatedDate,120),char(10)))
	, iif(C.LastUpdatedByEmail = '' or C.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',C.LastUpdatedByEmail))
	) as 'contact-note'
, CN.Note as 'contact-comment'
, CF.ConFiles as 'contact-document'
from Contact C
left join ContactNote CN on C.DynamicDataId = CN.ParentId
left join ContactFile CF on C.DynamicDataId = CF.DynamicDataId
left join ContactEmailFinal CEF on C.DynamicDataId = CEF.DynamicDataId 
order by C.DynamicDataId