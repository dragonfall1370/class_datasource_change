---Combine all notes for Job
with AllNote as (select N.NoteID, N.Type, N.Source, N.Text, N.CreatedByUserID, N.DateCreated, U.Email, U.DisplayName
from Note N
left join [User] U on U.UserID = N.CreatedByUserID)

, JobNote as (select JN.JobOrderID, JN.NoteID, N.Type, N.Source, N.Text
	, convert(varchar,N.DateCreated,120) as DateCreated, N.Email, N.DisplayName
	from JobOrderNote JN
	left join AllNote N on JN.NoteID = N.NoteID)

, JobNoteFinal as (SELECT
     JobOrderID,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar,DateCreated,120) + ' || ' 
		 + 'Created by: ' + Email + ' - ' + DisplayName + ' || ' + 'Type: ' + Type + ' || ' + Text
          from  JobNote
          WHERE JobOrderID = a.JobOrderID
		  order by DateCreated desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM JobNote as a
GROUP BY a.JobOrderID)

----Combine note attachment for each category
, NoteAttach as (select NoteID, AttachmentID, concat(AttachmentID,'.original',
case when FileType = '' then ''
when FileType = 'application/msword' then '.doc'
when FileType = 'application/pdf' then '.pdf'
when FileType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' then '.xlsx'
when FileType = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' then '.docx'
when FileType = 'image/jpeg' then '.jpg'
when FileType = 'image/png' then '.png'
else '' end) as FileName
from NoteAttachment
where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf' or FileName like '%jpg' or FileName like '%png')

, JobNoteAttach as (select JN.JobOrderID, JN.NoteID, NA.AttachmentID, NA.FileName
	from JobOrderNote JN
	left join NoteAttach NA on JN.NoteID = NA.NoteID
	where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf')

, JobNoteAttaches as (SELECT
     JobOrderID,
     STUFF(
         (SELECT ',' + FileName
          from  JobNoteAttach
          WHERE JobOrderID = a.JobOrderID
		  order by AttachmentID desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM JobNoteAttach as a
GROUP BY a.JobOrderID)
---Combine all documents >> no document for job

---If contactID is empty or null, get max contactID
, ContactMaxID as (select coalesce(CompanyID,9999999) as CompanyID, max(ContactID) as ContactMaxID from Contact 
where IsCandidateOnly = 0
group by CompanyID)

---Combine JobOrderCustomField
, JobCustomField as (SELECT
     JobOrderID,
     STUFF(
         (SELECT ',' + ValueText
          from  JobOrderCustomField
          WHERE JobOrderID = a.JobOrderID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM JobOrderCustomField as a
GROUP BY a.JobOrderID)

---Main script
select iif(J.ContactID = '' or J.ContactID is NULL,concat('IH',CM.ContactMaxID),concat('IH',J.ContactID)) as 'position-contactId'
, J.ContactID
, J.CompanyID
, concat('IH',J.JobOrderID) as 'position-externalId'
, J.JobTitle as 'position-title'
----job headcount = 1 as default
, case when W.Name = 'Permanent' then 'PERMANENT'
when W.Name = 'Temp' then 'TEMPORARY'
when W.Name = 'Contract' then 'CONTRACT'
else '' end as 'position-type'
, J.SalaryCurrency as 'position-currency'
, U.Email as 'position-owners'
, convert(nvarchar(10),J.StartDate,120) as 'position-startDate'
, J.SalaryMaxValue as 'position-actualSalary'
, J.JobDescription as 'position-publicDescription'
, left(concat('Job External ID: ',convert(varchar,J.JobOrderID),char(10)
	, iif(J.DateCreated = '' or J.DateCreated is NULL,'',concat('Created Date: ',convert(varchar,J.DateCreated,120),char(10)))
	, iif(JST.Name = '' or JST.Name is NULL,'',concat('Status: ',JST.Name,char(10)))
	, iif(convert(varchar,J.CompanyID) = '' or J.CompanyID is NULL,'',concat('Company: ',C.Name,char(10)))
	, iif(convert(varchar,J.ContactID) = '' or J.ContactID is NULL,'',concat('Contact: ',CON.FullName,char(10)))
	, iif(J.Source = '' or J.Source is NULL,'',concat('Source: ',J.Source,char(10)))
	, iif(convert(varchar,J.CategoryID) = '' or J.CategoryID is NULL,'',concat('Category: ',convert(varchar,J.CategoryID),'-',CG.Name,char(10)))
	, iif(convert(varchar,J.SubCategoryID) = '' or J.SubCategoryID is NULL,'',concat('SubCategory: ',convert(varchar,J.SubCategoryID),'-',SCG.Name,char(10)))
	, iif(convert(varchar,J.LocationID) = '' or J.LocationID is NULL,'',concat('Locations: ',L.Name,char(10)))
	, iif(J.StartDateType = '' or J.StartDateType is NULL,'',concat('Start Date Type: ',J.StartDateType,char(10)))
	, iif(J.SalaryType = '' or J.SalaryType is NULL,'',concat('SalaryType: ',J.SalaryType,char(10)))
	, iif(convert(varchar,J.SalaryMinValue) = '' or J.SalaryMinValue is NULL,'',concat('Salary Min Value: ',convert(varchar,J.SalaryMinValue),char(10)))
	, iif(convert(varchar,J.SalaryMaxValue) = '' or J.SalaryMaxValue is NULL,'',concat('Salary Max Value: ',convert(varchar,J.SalaryMaxValue),char(10)))
	, iif(convert(varchar,J.SalaryTimePerWeek) = '' or J.SalaryTimePerWeek is NULL,'',concat('Salary Time Per Week: ',convert(varchar,J.SalaryTimePerWeek),char(10)))
	, iif(J.FeeRateType = '' or J.FeeRateType is NULL,'',concat('Fee ate Type: ',J.FeeRateType,char(10)))
	, iif(convert(varchar,J.FeeRate) = '' or J.FeeRate is NULL,'',concat('Fee Rate: ',convert(varchar,J.FeeRate),char(10)))
	, iif(convert(varchar,J.FeeAmount) = '' or J.FeeAmount is NULL,'',concat('Fee Amount: ',convert(varchar,J.FeeAmount),char(10)))
	, iif(J.PercentageClose = '' or J.PercentageClose is NULL,'',concat('Percentage Close: ',J.PercentageClose,char(10)))
	, iif(JCF.URLList = '' or JCF.URLList is NULL,'',concat('Custom field values: ',JCF.URLList,char(10)))
	, iif(J.DateUpdated = '' or J.DateUpdated is NULL,'',concat('Last Updated: ',convert(varchar(10),J.DateUpdated,120),char(10)))
	),32000) as 'position-note'
, left(replace(replace(replace(JNF.URLList,'&lt;','<'),'&gt;','>'),'&amp;','&'),32000) as 'position-comment'
, REPLACE(JNA.URLList,'&amp;','&') as 'position-document'
from JobOrder J
left join ContactMaxID CM on CM.CompanyID = J.CompanyID
left join WorkType W on W.WorkTypeID = J.WorkTypeID --job work type
left join [User] U on U.UserID = J.OwnerUserID --job ownerUserID
left join JobOrderStatus JST on JST.StatusID = J.StatusID --job order status
left join Company C on C.CompanyID = J.CompanyID
left join Contact CON on CON.ContactID = J.ContactID
left join Category CG on CG.CategoryID = J.CategoryID
left join SubCategory SCG on SCG.SubCategoryID = J.SubCategoryID
left join [Location] L on L.LocationID = J.LocationID
left join JobCustomField JCF on JCF.JobOrderID = J.JobOrderID
left join JobNoteFinal JNF on JNF.JobOrderID = J.JobOrderID
left join JobNoteAttaches JNA on JNA.JobOrderID = J.JobOrderID