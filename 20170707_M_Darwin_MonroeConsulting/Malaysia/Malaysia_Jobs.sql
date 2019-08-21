with VacNote as (select n.ParentId, n.Text, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, n.CreatedDate 
from Note n
left join Users u on n.CreatedBy = u.Id)

, VacancyNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar(10),CreatedDate,120) + ' || ' + 'Created by: ' + CreatedByName + ' || ' + Text
          from  VacNote
          WHERE ParentId = a.ParentId
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

select concat('MCMalaysia',V.ContactId) as 'position-contactId'
, concat('MCMalaysia',V.DynamicDataId) as 'position-externalId'
, V.JobTitle as 'position-title'
, V.NumberofPositions as 'position-headcount'
, V.VacancyType as 'position-type'
, concat(coalesce('Requirements: ' + V.Requirements + char(10),''),coalesce('SkillSet: ' + V.SkillSet + char(10),'')
, coalesce('JobSpecNotes: ' + V.JobSpecNotes + char(10),''), coalesce('Other: ' + V.Other + char(10),'')) as 'position-publicDescription'
, concat(coalesce('Requirements: ' + V.Requirements + char(10),''),coalesce('SkillSet: ' + V.SkillSet + char(10),'')
, coalesce('JobSpecNotes: ' + V.JobSpecNotes + char(10),''), coalesce('Other: ' + V.Other + char(10),'')) as 'position-internalDescription'
, convert(varchar(10),V.CreatedDate,120) as 'position-startDate'
, left(concat('Vacancy External ID: ',V.DynamicDataId,char(10)
, iif(V.CreatedDate = '' or V.CreatedDate is NULL,'',concat('Created Date: ',convert(varchar(10),V.CreatedDate,120),char(10)))
, iif(V.Status = '' or V.Status is NULL,'',concat('Status: ',V.Status,char(10)))
, iif(V.Consultant = '' or V.Consultant is NULL,'',concat('Consultant: ',V.Consultant,char(10)))
, iif(V.Contact = '' or V.Contact is NULL,'',concat('Contact: ',V.Contact,char(10)))
, iif(V.Source = '' or V.Source is NULL,'',concat('Source: ',V.Source,char(10)))
, iif(V.NumberofPositions = '' or V.NumberofPositions is NULL,'',concat('Number of Positions: ',V.Source,char(10)))
, iif(V.JobType = '' or V.JobType is NULL,'',concat('Job Type: ',V.JobType,char(10)))
, iif(V.RateTo = '' or V.RateTo is NULL,'',concat('Rate to: ',V.RateTo,char(10)))
, iif(V.CurrencyCode = '' or V.CurrencyCode is NULL,'',concat('Currency code: ',V.CurrencyCode,char(10)))
, iif(V.IssuedOn = '' or V.IssuedOn is NULL,'',concat('Issued On: ',V.IssuedOn,char(10)))
, iif(V.Country = '' or V.Country is NULL,'',concat('Country: ',V.Country,char(10)))
, iif(V.BuildingName = '' or V.BuildingName is NULL,'',concat('Building Name: ',V.BuildingName,char(10)))
, iif(V.Add1 = '' or V.Add1 is NULL,'',concat('Add1: ',V.Add1,char(10)))
, iif(V.Add2 = '' or V.Add2 is NULL,'',concat('Add2: ',V.Add2,char(10)))
, iif(V.Town = '' or V.Town is NULL,'',concat('Town: ',V.Town,char(10)))
, iif(V.PostCode = '' or V.PostCode is NULL,'',concat('PostCode: ',V.PostCode,char(10)))
, iif(V.ContractLength = '' or V.ContractLength is NULL,'',concat('Contract Length: ',V.ContractLength,char(10)))
, iif(V.ContractRate = '' or V.ContractRate is NULL,'',concat('Contract Rate: ',V.ContractRate,char(10)))
, iif(V.Fee = '' or V.Fee is NULL,'',concat('Fee: ',V.Fee,char(10)))
, iif(V.qa_checkedBy = '' or V.qa_checkedBy is NULL,'',concat('qa_checkedBy: ',V.qa_checkedBy,char(10)))
, iif(V.qa_dateChecked = '' or V.qa_dateChecked is NULL,'',concat('qa_dateChecked: ',V.qa_dateChecked,char(10)))
, iif(V.qa_info = '' or V.qa_info is NULL,'',concat('qa_info: ',V.qa_info,char(10)))
, iif(V.LastUpdatedDate = '' or V.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),V.LastUpdatedDate,120),char(10)))
, iif(V.LastUpdatedByEmail = '' or V.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',V.LastUpdatedByEmail))
),32000) as 'position-note'
, left(replace(replace(replace(VN.Note,'&lt;','<'),'&gt;','>'),'&amp;','&'),32000) as 'position-comment'
, REPLACE(VF.JobFiles,'&amp;','&') as 'position-document'
from Vacancy V
left join VacancyNote VN on V.DynamicDataId = VN.ParentId
left join VacancyFile VF on V.DynamicDataId = VF.DynamicDataId
order by V.DynamicDataId