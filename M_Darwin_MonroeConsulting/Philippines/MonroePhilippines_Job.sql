with VacNote as (select n.ParentId, n.Text, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, n.Id 
from Note n
left join Users u on n.CreatedBy = u.Id)

, VacancyNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT char(10) + 'Created by: ' + CreatedByName + ' || ' + Text
          from  VacNote
          WHERE ParentId = a.ParentId
		  order by Id desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM VacNote as a
GROUP BY a.ParentId)

, DocumentEdited as (select DynamicDataId, replace(Filename,',','') as Filename from Document)

, VacancyFile (DynamicDataId, JobFiles)
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

--If Contact is empty, get Contact max ID within that company as contact reference
, ContactMaxID as (select CompayId, max(DynamicDataId) as ContactMaxID from Contact group by CompayId)

select iif(V.ContactId = '' or V.ContactId is NULL,concat('MCPhilippines',CM.ContactMaxID),concat('MCPhilippines',V.ContactId)) as 'position-contactId'
, V.CompanyId as 'CompanyID'
, V.ContactId as 'ContactID'
, concat('MCPhilippines',V.DynamicDataId) as 'position-externalId'
, V.JobTitle as 'position-title'
, iif(V.NumberofPositions = 0 or V.NumberofPositions is NULL or V.NumberofPositions = '',1,V.NumberofPositions) as 'position-headcount'
, case 
when V.VacancyType = 'Contract' or V.VacancyType = 'Fixed Term' or V.VacancyType = 'Fixed Term Contract' then 'CONTRACT'
when V.VacancyType = 'Project for 2 years' then 'INTERIM_PROJECT_CONSULTING'
else 'PERMANENT' end as 'position-type'
, 'PHP' as 'position-currency'
, concat(coalesce('Description: ' + V.Description + char(10),''),coalesce('SkillSet: ' + V.SkillSet + char(10),'')
, coalesce('JobSpecNotes: ' + V.JobSpecNotes + char(10),''), coalesce('Other: ' + V.Other,'')) as 'position-publicDescription'
, concat(coalesce('Description: ' + V.Description + char(10),''),coalesce('SkillSet: ' + V.SkillSet + char(10),'')
, coalesce('JobSpecNotes: ' + V.JobSpecNotes + char(10),''), coalesce('Other: ' + V.Other,'')) as 'position-internalDescription'
--, convert(varchar(10),V.CreatedDate,120) as 'position-startDate'
, left(concat('Vacancy External ID: ',V.DynamicDataId,char(10)
, iif(V.CreatedDate = '' or V.CreatedDate is NULL,'',concat('Created Date: ',V.CreatedDate,char(10)))
, iif(V.VacancyStatus = '' or V.VacancyStatus is NULL,'',concat('Status: ',V.VacancyStatus,char(10)))
, iif(V.Consultant = '' or V.Consultant is NULL,'',concat('Consultant: ',V.Consultant,char(10)))
, iif(V.Contact = '' or V.Contact is NULL,'',concat('Contact: ',V.Contact,char(10)))
, iif(V.Source = '' or V.Source is NULL,'',concat('Source: ',V.Source,char(10)))
, iif(V.NumberofPositions = '' or V.NumberofPositions is NULL,'',concat('Number of Positions: ',V.Source,char(10)))
, iif(V.VacancyType = '' or V.VacancyType is NULL,'',concat('Vacancy Type: ',V.VacancyType,char(10)))
, iif(V.RateTo = '' or V.RateTo is NULL,'',concat('Rate to: ',V.RateTo,char(10)))
, iif(V.CurrencyCode = '' or V.CurrencyCode is NULL,'',concat('Currency code: ',V.CurrencyCode,char(10)))
, iif(V.IssuedOn = '' or V.IssuedOn is NULL,'',concat('Issued On: ',V.IssuedOn,char(10)))
, iif(V.Country = '' or V.Country is NULL,'',concat('Country: ',V.Country,char(10)))
, iif(V.BuildingName = '' or V.BuildingName is NULL,'',concat('Building Name: ',V.BuildingName,char(10)))
, iif(V.Add1 = '' or V.Add1 is NULL,'',concat('Add1: ',V.Add1,char(10)))
, iif(V.Add2 = '' or V.Add2 is NULL,'',concat('Add2: ',V.Add2,char(10)))
, iif(V.Town = '' or V.Town is NULL,'',concat('Town: ',V.Town,char(10)))
, iif(V.City = '' or V.City is NULL,'',concat('City: ',V.City,char(10)))
, iif(V.County = '' or V.County is NULL,'',concat('County: ',V.County,char(10)))
, iif(V.PostCode = '' or V.PostCode is NULL,'',concat('PostCode: ',V.PostCode,char(10)))
, iif(V.ContractLength = '' or V.ContractLength is NULL,'',concat('Contract Length: ',V.ContractLength,char(10)))
, iif(V.ContractRate = '' or V.ContractRate is NULL,'',concat('Contract Rate: ',V.ContractRate,char(10)))
, iif(V.Hours = '' or V.Hours is NULL,'',concat('Hours: ',V.Hours,char(10)))
, iif(V.Fee = '' or V.Fee is NULL,'',concat('Fee: ',V.Fee,char(10)))
, iif(V.checkeddate = '' or V.checkeddate is NULL,'',concat('checkeddate: ',V.checkeddate,char(10)))
, iif(V.checked = '' or V.checked is NULL,'',concat('checked: ',V.checked,char(10)))
, iif(V.QAinfo = '' or V.QAinfo is NULL,'',concat('QAinfo: ',V.QAinfo,char(10)))
, iif(V.LastUpdatedDate = '' or V.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),V.LastUpdatedDate,120),char(10)))
, iif(V.LastUpdatedByEmail = '' or V.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',V.LastUpdatedByEmail))
),32000) as 'position-note'
, left(replace(replace(replace(VN.Note,'&lt;','<'),'&gt;','>'),'&amp;','&'),32000) as 'position-comment'
, REPLACE(VF.JobFiles,'&amp;','&') as 'position-document'
from Vacancy V
left join ContactMaxID CM on CM.CompayId = V.CompanyId
left join VacancyNote VN on V.DynamicDataId = VN.ParentId
left join VacancyFile VF on V.DynamicDataId = VF.DynamicDataId
order by V.DynamicDataId