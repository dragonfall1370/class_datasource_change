with VacNote as (select n.ParentId, n.Text, n.CreatedDate, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, u.Id
from Notes n
left join Users u on n.CreatedBy = u.Id)

, VacancyNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT '<hr>' + 'Created date: ' + convert(varchar(20),CreatedDate,120) + char(10) + 'Created by: ' + CreatedByName + ' || ' + Text
          from  VacNote
          WHERE ParentId = a.ParentId
		  order by CreatedDate desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 4, '')  AS URLList
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
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM DocumentEdited as a
where Filename not like '%.png%' and Filename not like '%.jpg%'
GROUP BY a.DynamicDataId)

--If Contact is empty, get max ContactID within reference
, ContactMaxID as (select 
case when CompanyID is NULL then '9999999'
else CompanyID end as CompanyID
, max(DynamicDataId) as ContactMaxID 
from Contact
group by CompanyID)

--DUPLICATION REGCONITION
, dup as (SELECT DynamicDataId, JobTitle, ROW_NUMBER() OVER(PARTITION BY JobTitle ORDER BY DynamicDataId ASC) AS rn 
FROM Vacancy)

--MAIN SCRIPT
select 
case 
	when (V.ContactID = '' or V.ContactID is NULL) and V.CompanyID in (select CompanyID from ContactMaxID) then concat('MCThailand',CM.ContactMaxID)
	when (V.ContactID = '' or V.ContactID is NULL) and V.CompanyID not in (select CompanyID from ContactMaxID) then 'MCThailand9999999'
	when V.ContactID is NULL and V.CompanyID is NULL then 'MCThailand9999999'
	else concat('MCThailand',V.ContactID) end as 'position-contactId'
, V.CompanyId as 'CompanyID'
, V.ContactId as 'ContactID'
, concat('MCThailand',V.DynamicDataId) as 'position-externalId'
, V.JobTitle as 'position-title(old)'
, iif(V.DynamicDataId in (select DynamicDataId from dup where dup.rn > 1)
	, iif(dup.JobTitle = '' or dup.JobTitle is NULL,concat('No job title-',dup.DynamicDataId),concat(dup.JobTitle,'-',dup.DynamicDataId,'-',dup.rn))
	, iif(V.JobTitle = '' or V.JobTitle is null,concat('No job title -',V.DynamicDataId),V.JobTitle)) as 'position-title'
, case 
when V.VacancyType in ('Contract', 'Fixed Term', 'Fixed Term Contract', 'Contract- 2 years', 'Perm. or Cont.') then 'CONTRACT'
when V.VacancyType = 'c) Interim' then 'INTERIM_PROJECT_CONSULTING'
else 'PERMANENT' end as 'position-type'
, 'THB' as 'position-currency'
, ltrim(stuff((coalesce(char(10) + 'Requirements: ' + V.Requirements,'') + coalesce(char(10) + 'SkillSet: ' + V.SkillSet,'') + coalesce(char(10) + 'JobSpecNotes: ' + V.JobSpecNotes,'') 
	+ coalesce(char(10) + 'Other: ' + V.Other,'') + coalesce(char(10) + 'Number of Positions: ' + V.NumberofPositions,'')),1,1,'')) as 'position-internalDescription'
, left(concat('Vacancy External ID: ',V.DynamicDataId,char(10)
	, iif(V.CreatedDate = '' or V.CreatedDate is NULL,'',concat('Created Date: ',V.CreatedDate,char(10)))
	, iif(V.VacancyStatus = '' or V.VacancyStatus is NULL,'',concat('Status: ',V.VacancyStatus,char(10)))
	, iif(V.Consultant = '' or V.Consultant is NULL,'',concat('Consultant: ',V.Consultant,char(10)))
	, iif(V.Contact = '' or V.Contact is NULL,'',concat('Contact: ',V.Contact,char(10)))
	, iif(V.Company = '' or V.Company is NULL,'',concat('Company: ',V.Company,char(10)))
	, iif(V.Source = '' or V.Source is NULL,'',concat('Source: ',V.Source,char(10)))
	, iif(V.NumberofPositions = '' or V.NumberofPositions is NULL,'',concat('Number of Positions: ',V.NumberofPositions,char(10)))
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
	, iif(V.Hours = '' or V.Hours is NULL,'',concat('Hours: ',V.Hours,char(10)))
	, iif(V.Fee = '' or V.Fee is NULL,'',concat('Fee: ',V.Fee,char(10)))
	, iif(V.qa_dateChecked = '' or V.qa_dateChecked is NULL,'',concat('qa_dateChecked: ',V.qa_dateChecked,char(10)))
	, iif(V.qa_checkedBy = '' or V.qa_checkedBy is NULL,'',concat('qa_checkedBy: ',V.qa_checkedBy,char(10)))
	, iif(V.qa_info = '' or V.qa_info is NULL,'',concat('qa_info: ',V.qa_info,char(10)))
	, iif(V.LastUpdatedDate = '' or V.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),V.LastUpdatedDate,120),char(10)))
	, iif(V.LastUpdatedByEmail = '' or V.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',V.LastUpdatedByEmail))
	),32000) as 'position-note'
, VN.Note as 'position-comment'
, VF.JobFiles as 'position-document'
from Vacancy V
left join ContactMaxID CM on CM.CompanyID = V.CompanyID
left join VacancyNote VN on V.DynamicDataId = VN.ParentId
left join VacancyFile VF on V.DynamicDataId = VF.DynamicDataId
left join dup on V.DynamicDataId = dup.DynamicDataId
order by V.DynamicDataId