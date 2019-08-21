---Combine all notes for Candidate
with AllNote as (select N.NoteID, N.Type, N.Source, N.Text, N.CreatedByUserID, N.DateCreated, U.Email, U.DisplayName
from Note N
left join [User] U on U.UserID = N.CreatedByUserID)

, CandNote as (select CN.ContactID, CN.NoteID, N.Type, N.Source, N.Text
	, convert(varchar,N.DateCreated,120) as DateCreated, N.Email, N.DisplayName
	from CandidateNote CN
	left join AllNote N on CN.NoteID = N.NoteID)

, CandNoteFinal as (SELECT
     ContactID,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar,DateCreated,120) + ' || ' 
		 + 'Created by: ' + Email + ' - ' + DisplayName + ' || ' + 'Type: ' + Type + ' || ' + Text
          from  CandNote
          WHERE ContactID = a.ContactID
		  order by DateCreated desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandNote as a
GROUP BY a.ContactID)

----Combine note attachment for each category
, NoteAttach as (select NoteID, AttachmentID, concat(AttachmentID,'.original',
case
when FileType = 'application/msword' then '.doc'
when FileType = 'application/pdf' then '.pdf'
when FileType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' then '.xlsx'
when FileType = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' then '.docx'
when FileType = 'image/jpeg' then '.jpg'
when FileType = 'image/png' then '.png'
else '' end) as FileName
from NoteAttachment
where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf' or FileName like '%jpg' or FileName like '%png')

, CandNoteAttach as (select CN.ContactID, CN.NoteID, NA.AttachmentID, NA.FileName
	from CandidateNote CN
	left join NoteAttach NA on CN.NoteID = NA.NoteID
	where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf')

, CandNoteAttaches as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  CandNoteAttach
          WHERE ContactID = a.ContactID
		  order by AttachmentID desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandNoteAttach as a
GROUP BY a.ContactID)

---Combine all documents for Candidate: 13 files cannot be found

---Combine all CandidateAttachment-ProcessedText as candidate' summary
, CandSummary as (SELECT
     ContactID,
     STUFF(
         (SELECT char(10) + ProcessedText
          from  CandidateAttachment
          WHERE ContactID = a.ContactID
		  order by AttachmentID desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandidateAttachment as a
GROUP BY a.ContactID)

---Candidate owner user in Contact
, CandOwner as (select C.ContactID, U.Email 
from Contact C
left join [User] U on U.UserID = C.OwnerUserID)

---Combine all candidate attachments
, CandAttachmentEdit as (SELECT ContactID, 
	concat(AttachmentID,'.original.',right(FileName,len(FileName)-charindex('.',FileName))) as FileName
from CandidateAttachment
where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf')

, CandAttachmentFinal as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  CandAttachmentEdit
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandAttachmentEdit as a
GROUP BY a.ContactID)

---Main script
select top 100 concat('GS',C.ContactID) as 'candidate-externalId'
, coalesce(CON.FirstName,'Firstname') as 'candidate-firstName'
, coalesce(CON.LastName,concat('Lastname-',C.ContactID)) as 'candidate-Lastname'
, upper(CON.Salutation) as 'candidate-title'--no data for IdealHealthcare
, coalesce(CON.Email,concat('candidate-',C.ContactID,'@noemail.com')) as 'candidate-email' --Contact > Email will be Primary
, CON.Mobile as 'candidate-phone'
, CON.Phone as 'candidate-workPhone'
, concat(iif(C.AddressLine1 = '' or C.AddressLine1 is NULL,'',concat(C.AddressLine1,', '))
	, iif(C.AddressLine2 = '' or C.AddressLine2 is NULL,'',concat(C.AddressLine2,', '))
	, iif(C.AddressSuburb = '' or C.AddressSuburb is NULL,'',concat(C.AddressSuburb,', '))
	, iif(C.AddressState = '' or C.AddressState is NULL,'',concat(C.AddressState,', '))
	, iif(C.AddressPostcode = '' or C.AddressPostcode is NULL,'',concat(C.AddressPostcode,', '))
	, iif(C.AddressCountry = '' or C.AddressCountry is NULL,'',C.AddressCountry)) as 'candidate-address'
, C.AddressSuburb as 'candidate-city'
, C.AddressPostcode as 'candidate-zipCode'
, C.AddressCountry as '(AddressCountry)'
, case 
when C.AddressCountry = 'Austria' then 'AT'
when C.AddressCountry = 'Belgium' then 'BE'
when C.AddressCountry = 'China' then 'CN'
when C.AddressCountry = 'Czech Republic' then 'CZ'
when C.AddressCountry = 'Germany' then 'DE'
when C.AddressCountry = 'Ireland' then 'IE'
when C.AddressCountry = 'Kuwait' then 'KW'
when C.AddressCountry = 'Netherlands' then 'NL'
when C.AddressCountry = 'Romania' then 'RO'
when C.AddressCountry = 'South Korea' then 'KR'
when C.AddressCountry = 'Sweden' then 'SE'
when C.AddressCountry = 'United Kingdom' then 'GB'
when C.AddressCountry = 'Cyprus' then 'CY'
when C.AddressCountry = 'Denmark' then 'DK'
when C.AddressCountry = 'Hungary' then 'HU'
when C.AddressCountry = 'Japan' then 'JP'
when C.AddressCountry = 'Monaco' then 'MC'
when C.AddressCountry = 'Norway' then 'NO'
when C.AddressCountry = 'Poland' then 'PL'
when C.AddressCountry = 'Thailand' then 'TH'
when C.AddressCountry = 'Australia' then 'AU'
when C.AddressCountry = 'Bahrain' then 'BH'
when C.AddressCountry = 'Canada' then 'CA'
when C.AddressCountry = 'Canada Area' then 'CA'
when C.AddressCountry = 'Egypt' then 'EG'
when C.AddressCountry = 'Greece' then 'GR'
when C.AddressCountry = 'India' then 'IN'
when C.AddressCountry = 'Indonesia' then 'ID'
when C.AddressCountry = 'Luxembourg' then 'LU'
when C.AddressCountry = 'Nigeria' then 'NG'
when C.AddressCountry = 'Portugal' then 'PT'
when C.AddressCountry = 'Qatar' then 'QA'
when C.AddressCountry = 'Saudi Arabia' then 'SA'
when C.AddressCountry = 'South Africa' then 'ZA'
when C.AddressCountry = 'Sri Lanka' then 'LK'
when C.AddressCountry = 'United Arab Emirates' then 'AE'
when C.AddressCountry = 'United States' then 'US'
when C.AddressCountry = 'Vietnam' then 'VN'
when C.AddressCountry = 'Finland' then 'FI'
when C.AddressCountry = 'France' then 'FR'
when C.AddressCountry = 'Hong Kong' then 'HK'
when C.AddressCountry = 'Italy' then 'IT'
when C.AddressCountry = 'Malaysia' then 'MY'
when C.AddressCountry = 'Myanmar' then 'MM'
when C.AddressCountry = 'New Zealand' then 'NZ'
when C.AddressCountry = 'Pakistan' then 'PK'
when C.AddressCountry = 'Philippines' then 'PH'
when C.AddressCountry = 'Sierra Leone' then 'SL'
when C.AddressCountry = 'Singapore' then 'SG'
when C.AddressCountry = 'Spain' then 'ES'
when C.AddressCountry = 'Switzerland' then 'CH'
when C.AddressCountry = 'Taiwan' then 'TW'
ELSE '' end as 'candidate-country'
, C.CurrentEmployer as 'candidate-employer1'
, CON.Position as 'candidate-jobTitle1'
, C.WorkTypeID
, concat(coalesce('Current Employer: ' + C.CurrentEmployer + char(10),''),coalesce('Current Position: ' + CON.Position + char(10),'')
	,iif(C.WorkTypeID = '' or C.WorkTypeID is NULL,'',concat('Current Work Type: ',WT.Name,char(10)))
	,coalesce('Current Salary Type: ' + C.CurrentSalaryType + char(10),'')
	,coalesce('Current Salary Currency: ' + C.CurrentSalaryCurrency + char(10),'')
	,coalesce('Current Salary Value: ' + convert(varchar,C.CurrentSalaryValue),'')) as 'candidate-company1'
, concat(coalesce('Current Employer: ' + C.CurrentEmployer + char(10),'')
	,coalesce('Current Position: ' + CON.Position + char(10),'')
	,iif(convert(varchar,C.WorkTypeID) = '' or C.WorkTypeID is NULL,'',concat('Current Work Type: ',WT.Name,char(10)))
	,coalesce('Current Salary Type: ' + C.CurrentSalaryType + char(10),'')
	,coalesce('Current Salary Currency: ' + C.CurrentSalaryCurrency + char(10),'')
	,coalesce('Current Salary Value: ' + convert(varchar,C.CurrentSalaryValue) + char(10),'')
	,coalesce('Current Seeking Status: ' + C.CurrentSeekingStatus + char(10),'')
	,coalesce('Current Seeking Status Date Updated: ' + convert(varchar,C.CurrentSeekingStatusDateUpdatedUtc),'')) as 'candidate-workHistory'
, C.SalaryCurrency as 'candidate-currency'
, C.CurrentSalaryValue as 'candidate-currentSalary'
, C.SalaryMaxValue as 'candidate-desiredSalary'
, left(CON.LinkedInUrl,254) as 'candidate-linkedInUrl'
, concat(coalesce(CNA.URLList + ',',''),coalesce(CAF.URLList,'')) as 'candidate-resume'
, CO.Email as 'candidate-owners'
, left(concat('Candidate External ID: ',C.ContactID,char(10)
	,iif(CON.FullName = '' or CON.FullName is NULL,'',concat('Full Name: ',CON.FullName,char(10)))
	,iif(C.DateCreated = '' or C.DateCreated is NULL,'',concat('Created Date: ',convert(varchar,C.DateCreated,120),char(10)))
	,iif(C.Source = '' or C.Source is NULL,'',concat('Source: ',C.Source,char(10)))
	,iif(convert(varchar,C.StatusID) = '' or C.StatusID is NULL,'',concat('Status: ',CS.Name,char(10)))
	,iif(C.IdealPosition = '' or C.IdealPosition is NULL,'',concat('Ideal Position: ',C.IdealPosition,char(10)))
	,iif(C.StartDateType = '' or C.StartDateType is NULL,'',concat('Start Date Type: ',C.StartDateType,char(10)))
	,iif(C.Summary = '' or C.Summary is NULL,'',concat('Summary: ',C.Summary,char(10)))
	,iif(C.SalaryType = '' or C.SalaryType is NULL,'',concat('Salary Type: ',C.SalaryType,char(10)))
	,iif(convert(varchar,C.SalaryMinValue) = '' or C.SalaryMinValue is NULL,'',concat('Salary Min Value: ',convert(varchar,C.SalaryMinValue),char(10)))
	,iif(convert(varchar,C.SalaryMaxValue) = '' or C.SalaryMaxValue is NULL,'',concat('Salary Max Value: ',convert(varchar,C.SalaryMaxValue),char(10)))
	,coalesce('Current Seeking Status: ' + C.CurrentSeekingStatus + char(10),'')
	,coalesce('Current Seeking Status Date Updated: ' + convert(varchar,C.CurrentSeekingStatusDateUpdatedUtc),'')
	,iif(C.LastNoteDate = '' or C.LastNoteDate is NULL,'',concat('Last Note Date: ',convert(varchar(10),C.LastNoteDate,120),char(10)))
	,iif(C.DateUpdated = '' or C.DateUpdated is NULL,'',concat('Last Updated: ',convert(varchar(10),C.DateUpdated,120)))
	,iif(CSU.URLList = '' or CSU.URLList is NULL,'',concat('Candidate Resume: ',CSU.URLList,char(10)))
	),32000) as 'candidate-note'
, left(replace(replace(replace(replace(CNF.URLList,'&lt;','<'),'&gt;','>'),'&amp;','&'),'&#x0D;',''),32000) as 'candidate-comments'
from Candidate C
left join Contact CON on CON.ContactID = C.ContactID
left join WorkType WT on WT.WorkTypeID = C.CurrentWorkTypeID
left join CandNoteAttaches CNA on CNA.ContactID = C.ContactID
left join CandOwner CO on CO.ContactID = C.ContactID
left join CandSummary CSU on CSU.ContactID = C.ContactID
left join CandNoteFinal CNF on CNF.ContactID = C.ContactID
left join CandidateStatus CS on CS.StatusID = C.StatusID
left join CandAttachmentFinal CAF on CAF.ContactID = C.ContactID