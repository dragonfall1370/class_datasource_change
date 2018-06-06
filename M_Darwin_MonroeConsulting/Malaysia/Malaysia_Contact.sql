with ConNote as (select n.ParentId, n.Text, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, n.CreatedDate 
from Note n
left join Users u on n.CreatedBy = u.Id)

, ContactNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar(10),CreatedDate,120) + ' || ' + 'Created by: ' + CreatedByName + ' || ' + Text
          from  ConNote
          WHERE ParentId = a.ParentId
		  order by CreatedDate desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
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
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM DocumentEdited as a
where Filename not like '%.png%' and Filename not like '%.jpg%'
GROUP BY a.DynamicDataId)

, ContactPhone as (SELECT 
	DynamicDataId,
	concat(iif(Tel = '' or Tel is NULL,'',concat(Tel,',')),
	iif(MobileCombine = '' or MobileCombine is NULL,'',MobileCombine)) as PrimaryPhone
	from Contact)

select iif(left(C.CompanyID,10)='MCMalaysia',C.CompanyID,concat('MCMalaysia',C.CompanyID)) as 'contact-companyId'
, concat('MCMalaysia',C.DynamicDataId) as 'contact-externalId'
, Coalesce(C.FirstName,'Firstname') as 'contact-firstName'
, Coalesce(C.LastName,concat('Lastname-',C.DynamicDataId)) as 'contact-lastName'
, ltrim(rtrim(C.EmailCombine)) as 'contact-email'
, iif(right(CP.PrimaryPhone,1)=',',left(CP.PrimaryPhone,len(CP.PrimaryPhone)-1),CP.PrimaryPhone) as 'contact-phone'
, C.Position as 'contact-jobTitle'
, concat('Contact External ID: ',C.DynamicDataId,char(10)
, iif(C.CreatedDate = '' or C.CreatedDate is NULL,'',concat('Created Date: ',convert(varchar(10),C.CreatedDate,120),char(10)))
, iif(C.Name = '' or C.Name is NULL,'',concat('Contact Name: ',C.Name,char(10)))
, iif(C.Fax = '' or C.Fax is NULL,'',concat('Fax: ',C.Fax,char(10)))
, iif(C.Extn = '' or C.Extn is NULL,'',concat('Extension: ',C.Extn,char(10)))
, iif(C.Division = '' or C.Division is NULL,'',concat('Division: ',C.Division,char(10)))
, iif(C.knownAs = '' or C.knownAs is NULL,'',concat('Known as: ',C.knownAs,char(10)))
, iif(C.Company = '' or C.Company is NULL,'',concat('Company name: ',C.Company,char(10)))
, iif(C.LastUpdatedDate = '' or C.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),C.LastUpdatedDate,120),char(10)))
, iif(C.LastUpdatedByEmail = '' or C.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',C.LastUpdatedByEmail))
) as 'contact-note'
, left(replace(replace(replace(CN.Note,'&lt;','<'),'&gt;','>'),'&amp;','&'),32000) as 'contact-comment'
, REPLACE(CF.ConFiles,'&amp;','&') as 'contact-document'
from Contact C
left join ContactPhone CP on C.DynamicDataId = CP.DynamicDataId
left join ContactNote CN on C.DynamicDataId = CN.ParentId
left join ContactFile CF on C.DynamicDataId = CF.DynamicDataId
order by C.DynamicDataId