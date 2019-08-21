--CANDIDATE PERSONAL EMAIL
/* Option 1: without splitting multiple emails within a cell
with PersonEmails as (select C.CandidateID, PE.PersonID, PE.EmailID
	, rtrim(ltrim(replace(replace(replace(replace(replace(Email.EmailAddress,' /',','),' ;',','),':',''),'-;',''),',@','@'))) as EmailAddress --> to remove special characters
	from Candidate C
	left join Person_Email PE on PE.PersonID = C.PersonID
	left join Email on Email.EmailID = PE.EmailID
	left join EmailType ET on ET.EmailTypeID = Email.EmailTypeID
	where Email.Deleted = 0 and EmailAddress like '%_@_%.__%'
	and C.Deleted = 0 and ET.EmailTypeID = 1) --> personal email type

, minPersonEmailID as (select CandidateID, min(EmailID) as minEmailID
	from PersonEmails
	group by CandidateID)

, minEmails as (select pe.CandidateID, mpe.minEmailID, pe.EmailAddress
	from minPersonEmailID mpe
	left join PersonEmails pe on pe.EmailID = mpe.minEmailID)

, maildup as (select CandidateID, EmailAddress, ROW_NUMBER() OVER(PARTITION BY EmailAddress ORDER BY CandidateID ASC) AS rn 
	from minEmails)

, CandidateEmail as (select CandidateID
	, case when rn = 1 then rtrim(ltrim(EmailAddress))
	else concat(rn,'-',EmailAddress) end as EmailAddress
	, rn 
	from maildup) */

/* Option 2: splitting multiple mails within a cell, in correct format */
with PersonEmails as (select C.CandidateID, PE.PersonID, PE.EmailID
	, rtrim(ltrim(Email.EmailAddress)) as EmailAddress
	from Candidate C
	left join Person_Email PE on PE.PersonID = C.PersonID
	left join Email on Email.EmailID = PE.EmailID
	left join EmailType ET on ET.EmailTypeID = Email.EmailTypeID
	where Email.Deleted = 0 and EmailAddress like '%_@_%.__%'
	and C.Deleted = 0) -- and ET.EmailTypeID = 1 | personal email type

, PersonCombinedEmails as (SELECT
     CandidateID, PersonID,
     STUFF(
         (SELECT ',' + EmailAddress
			from PersonEmails
			WHERE CandidateID = a.CandidateID
			FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
			, 1, 1, '')  AS PersonCombinedEmails
FROM PersonEmails as a
GROUP BY a.CandidateID, a.PersonID)

, PersonCombinedEmails2 as (select CandidateID, PersonID
	, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
	(replace(replace(replace(replace(replace(replace(PersonCombinedEmails,'/',' '),'<',' '),'>',' ')
	,'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' ')
	,'''',' '),';',' '),'•',' '),CHAR(9),' ') as PersonCombinedEmails
	from PersonCombinedEmails)

, PersonSplitEmails as (SELECT CandidateID, Split.a.value('.', 'VARCHAR(100)') AS PersonSplitEmails 
	FROM (SELECT CandidateID, CAST ('<M>' + REPLACE(REPLACE(PersonCombinedEmails,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data 
	FROM PersonCombinedEmails2) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))

, PersonSplitEmails2 as (SELECT CandidateID
	, case when RIGHT(PersonSplitEmails, 1) = '.' then LEFT(PersonSplitEmails, LEN(PersonSplitEmails) - 1) 
	when LEFT(PersonSplitEmails, 1) = '.' then RIGHT(PersonSplitEmails, LEN(PersonSplitEmails) - 1) 
	else PersonSplitEmails end as PersonSplitEmails 
	from PersonSplitEmails 
	WHERE PersonSplitEmails like '%_@_%.__%')

, PersonSplitEmails2Dup as (SELECT CandidateID, ltrim(rtrim(CONVERT(NVARCHAR(MAX), PersonSplitEmails))) as EmailAddress
	, ROW_NUMBER() OVER (PARTITION BY CandidateID ORDER BY CandidateID desc) as rn
	FROM PersonSplitEmails2)

--CANDIDATE OWNERS | instead of getting maxEmailID from Person_Email, get mail
/* , MaxRecruiterEmailID as (select R.RecruiterID, max(E.EmailID) as MaxEmailID 
	from Recruiter R
	left join Person_Email PE on PE.PersonID = R.PersonID
	left join Email E on E.EmailID = PE.EmailID
	where E.Deleted = 0
	group by R.RecruiterID)

, CandidateOwners as (select RC.CandidateID, RC.RecruiterID, MRE.MaxEmailID, Email.EmailAddress 
	from Recruiter_Candidate RC
	left join MaxRecruiterEmailID MRE on MRE.RecruiterID = RC.RecruiterID
	left join Email on Email.EmailID = MRE.MaxEmailID
	where RC.Deleted = 0) */

, CandidateOwners as (select RC.CandidateID, RC.RecruiterID, AU.Username as OwnerAddress
	from Recruiter_Candidate RC
	left join ApplicationUser AU on AU.RecruiterID = RC.RecruiterID
	where AU.Deleted = 0)

--CANDIDATE DEFAULT ADDRESS | Candidate default address can be found in Person table
, DefaultCandidateAddress as (select C.CandidateID, P.DefaultAddressID, A.AddressTypeID, AT.Description as AddressType, A.Street, A.Street2, A.CityId, City.Description as City
	, A.ProvinceStateId, PS.Description as Province, A.PostalZip, A.CountryId, CT.Description as Country
	from Candidate C
	left join Person P on P.PersonID = C.PersonID
	left join Address A on A.AddressID = P.DefaultAddressID
	left join AddressType AT on AT.AddressTypeID = A.AddressTypeID
	left join City on City.CityID = A.CityId
	left join ProvinceState PS on PS.ProvinceStateID = A.ProvinceStateId
	left join Country CT on CT.CountryId = A.CountryId
	where C.Deleted = 0) --> Candidate should not be deleted

--CANDIDATE CELLPHONE
, MaxPhones as (select C.CandidateID, max(PP.PhoneID) as maxPhoneID
	from Candidate C
	left join Person_Phone PP on PP.PersonID = C.PersonID and PP.Deleted = 0
	left join Phone on Phone.PhoneID = PP.PhoneID and Phone.Deleted = 0
	left join PhoneType PT on PT.PhoneTypeID = Phone.PhoneTypeID 
	where C.Deleted = 0 and Phone.PhoneTypeID = 3 --> this is for Cell phone - ID: 3
	group by C.CandidateID)

, CandidateCellPhone as (select MP.CandidateID, Phone.PhoneNumber
	from MaxPhones MP
	left join Phone on Phone.PhoneID = MP.maxPhoneID
	where Phone.PhoneNumber is not NULL)

--CANDIDATE PHONES >> other phones than cellphone
, OtherPhones as (select C.CandidateID, Phone.PhoneNumber
	from Candidate C
	left join Person_Phone PP on PP.PersonID = C.PersonID and PP.Deleted = 0
	left join Phone on Phone.PhoneID = PP.PhoneID and Phone.Deleted = 0
	left join PhoneType PT on PT.PhoneTypeID = Phone.PhoneTypeID 
	where C.Deleted = 0 and Phone.PhoneTypeID in (1,2,4,5)) --> get all phones other than cellphone)

, CandidatePhone as (SELECT
     CandidateID, 
     STUFF(
         (SELECT ', ' + PhoneNumber
          from OtherPhones
          WHERE CandidateID = a.CandidateID
		  and PhoneNumber is not NULL
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS CandidatePhone
FROM OtherPhones as a
where PhoneNumber is not NULL
GROUP BY a.CandidateID)

--CANDIDATE DOCUMENTS
, Documents as (select PD.PersonID, PD.DocumentID, D.DocumentFileName, D.DocumentTypeID, DT.Description
	from Person_Document PD
	left join Document D on D.DocumentID = PD.DocumentID
	left join DocumentType DT on DT.DocumentTypeID = D.DocumentTypeID
	where D.DocumentFileName like '%.pdf' or D.DocumentFileName like '%.rtf' or D.DocumentFileName like '%.doc%' 
	or D.DocumentFileName like '%.xls%' or D.DocumentFileName like '%.html')

, PersonDocuments as (SELECT
     PersonID,
     STUFF(
         (SELECT ', ' + replace(replace(replace(DocumentFileName,',',''),char(0x0008),''),char(0x0010),'')
          from  Documents
          WHERE PersonID = a.PersonID
		  order by DocumentTypeID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS PersonDocuments
FROM Documents as a
GROUP BY a.PersonID)

, CandidateDocumentFinal as (select C.CandidateID, PD.PersonDocuments as CandidateDocument
	from Candidate C
	left join PersonDocuments PD on PD.PersonID = C.PersonID
	where C.Deleted = 0 and PD.PersonDocuments is not NULL)

--CANDIDATE CUSTOM FIELDS
/* 
ApplicationLabelID: 1366 - Technical / Domain Experience (Minimum 5 words and 30 characters)
ApplicationLabelID: 1367 - Current Salary
ApplicationLabelID: 1368 - Expected Salary
ApplicationLabelID: 1369 - Outlook
ApplicationLabelID: 1501 - Presentation
ApplicationLabelID: 1502 - Push /Pull Factors (Minimum 5 words and 30 characters)
ApplicationLabelID: 1503 - Recent Interviews
*/

--CANDIDATE WORK HISTORY
, CandidateWorkHistory as (SELECT
     CandidateID, 
     STUFF(
         (SELECT char(10) + coalesce('Company name: ' + ceh.CompanyName + char(10),'') + coalesce('Title: ' + ceh.Title + char(10),'')
		 + coalesce('Start Date: ' + convert(varchar(10),ceh.StartDate,120) + char(10),'') 
		 + coalesce('End Date: ' + convert(varchar(10),ceh.EndDate,120) + char(10),'')
		 + coalesce('Duties:' + ceh.Duties + char(10),'') + coalesce('Reason for leaving:' + ceh.ReasonForLeaving + char(10),'')
		 + coalesce('Note:' + cast(note.NoteText as nvarchar(max)) + char(10),'')
          from CandidateEmploymentHistory ceh
		  left join Note on Note.NoteID = ceh.NoteID
          WHERE ceh.CandidateID = a.CandidateID
		  order by ceh.StartDate desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CandidateWorkHistory
FROM CandidateEmploymentHistory as a
GROUP BY a.CandidateID) --| This is for candidate work history

, CandidateEmpHistory as (select CandidateID, CompanyName, Title, StartDate, EndDate, ROW_NUMBER() OVER(PARTITION BY CandidateID ORDER BY StartDate desc) AS rn 
	from CandidateEmploymentHistory)

--CANDIDATE NOTES
, PersonNotes as (select C.CandidateID, PN.PersonID, PN.NoteID, Note.NoteTypeID, NT.Description, PN.EnteredDate, PN.ModifiedDate, PN.EnteredByID
	, P.FirstName as EnteredFirstName, P.LastName as EnteredLastName, Note.NoteText
	from Candidate C
	left join Person_Note PN on PN.PersonID = C.PersonID
	left join Note on Note.NoteID = PN.NoteID
	left join NoteType NT on NT.NoteTypeID = Note.NoteTypeID
	left join Recruiter R on R.RecruiterID = C.EnteredByID --> v2.RecruiterID instead of R.PersonID
	left join Person P on P.PersonID = R.PersonID
	where Note.Deleted = 0 and P.Deleted = 0)
	--and Note.NoteTypeID = 300) >>> NoteTypeID = 300 for Contact

, CandidateNoteFinal as (SELECT
     CandidateID,
     STUFF(
         (SELECT '<hr>' + coalesce('Note created: ' + convert(varchar(20),EnteredDate,120) + char(10),'') 
		 + coalesce('Entered By: ' + EnteredFirstName + ' ' + EnteredLastName + char(10),'')
		 + coalesce('Modified Date: ' + convert(varchar(20),ModifiedDate,120) + char(10),'')
		 + 'Notes: ' + replace(cast(NoteText as nvarchar(max)),' &amp; ',' & ')
          from PersonNotes
          WHERE CandidateID = a.CandidateID
		  and EnteredDate is not NULL
		  order by ModifiedDate desc
          FOR XML PATH (''))
          , 1, 4, '')  AS CandidateNoteFinal
FROM PersonNotes as a
where EnteredDate is not NULL
GROUP BY a.CandidateID)	
	
--MAIN SCRIPT
select 
concat('MSC',c.CandidateID) as 'candidate-externalId'
, upper(replace(s.Description,'.','')) as 'candidate-title'
, iif(p.FirstName = '' or p.FirstName is NULL,'Firstname',p.FirstName) as 'candidate-firstName'
, iif(p.LastName = '' or p.LastName is NULL,concat('LastName-',c.CandidateID),p.LastName) as 'candidate-lastName'
, coalesce(p.MiddleName,'') as 'candidate-middleName'
, iif(c.CandidateID in (select CandidateID from PersonSplitEmails2Dup),psm.EmailAddress,concat('candidate-',c.CandidateID,'@noemail.com')) as 'candidate-email'
, cp.CandidatePhone as 'candidate-phone'
, ccp.PhoneNumber as 'candidate-mobile'
, convert(varchar(10),csi.BirthDate,120) as 'candidate-dob'
, co.OwnerAddress as 'candidate-owners'
, replace(coalesce(dca.AddressType + ': ','') + stuff((coalesce(', ' + dca.Street,'') + coalesce(', ' + dca.Street2,'') + coalesce(', ' + dca.City,'')
	+ coalesce(', ' + dca.Province,'') + coalesce(', ' + dca.PostalZip,'') + coalesce(', ' + dca.Country,'')),1,2,''),',,',',') as 'candidate-address'
, dca.city as 'candidate-city'
, case
when dca.country like '%South Korea%' then 'KR'
when dca.country like '%DE%' then 'DE'
when dca.country like '%IN%' then 'IN'
when dca.country like '%CO%' then 'CO'
when dca.country like '%BE%' then 'BE'
when dca.country like '%PH%' then 'PH'
when dca.country like '%Italy%' then 'IT'
when dca.country like '%Jordan%' then 'JO'
when dca.country like '%Hong Kong%' then 'HK'
when dca.country like '%Malaysia%' then 'MY'
when dca.country like '%NG%' then 'NG'
when dca.country like '%Indonesia%' then 'ID'
when dca.country like '%Philippines%' then 'PH'
when dca.country like '%Switzerland%' then 'CH'
when dca.country like '%United States%' then 'US'
when dca.country like '%Nigeria%' then 'NG'
when dca.country like '%Australia%' then 'AU'
when dca.country like '%AU%' then 'AU'
when dca.country like '%TW%' then 'TW'
when dca.country like '%United Kingdom%' then 'GB'
when dca.country like '%HK%' then 'HK'
when dca.country like '%China%' then 'CN'
when dca.country like '%CA%' then 'CA'
when dca.country like '%DK%' then 'DK'
when dca.country like '%India%' then 'IN'
when dca.country like '%United Arab Emirates%' then 'AE'
when dca.country like '%UK%' then 'GB'
when dca.country like '%BR%' then 'BR'
when dca.country like '%ZA%' then 'ZA'
when dca.country like '%IE%' then 'IE'
when dca.country like '%Poland%' then 'PL'
when dca.country like '%Canada%' then 'CA'
when dca.country like '%PL%' then 'PL'
when dca.country like '%Thailand%' then 'TH'
when dca.country like '%France%' then 'FR'
when dca.country like '%ID%' then 'ID'
when dca.country like '%PT%' then 'PT'
when dca.country like '%Japan%' then 'JP'
when dca.country like '%MY%' then 'MY'
when dca.country like '%US%' then 'US'
when dca.country like '%Spain%' then 'ES'
when dca.country like '%JO%' then 'JO'
when dca.country like '%Papua New Guinea%' then 'PG'
when dca.country like '%RU%' then 'RU'
when dca.country like '%Bangladesh%' then 'BD'
when dca.country like '%Pakistan%' then 'PK'
when dca.country like '%Romania%' then 'RO'
when dca.country like '%Singapore%' then 'SG'
when dca.country like '%Colombia%' then 'CO'
when dca.country like '%Nepal%' then 'NP'
when dca.country like '%South Africa%' then 'ZA'
when dca.country like '%CN%' then 'CN'
when dca.country like '%Portugal%' then 'PT'
when dca.country like '%Taiwan%' then 'TW'
when dca.country like '%SG%' then 'SG'
else NULL end as 'candidate-Country'
, dca.PostalZip as 'candidate-zipCode'
, dca.Province as 'candidate-State'
, c.LinkedInID as 'candidate-linkedln'
, cli.BaseSalary as 'candidate-currentSalary'
, 'HKD' as 'candidate-currency'
--WORK HISTORY
, ceh.CompanyName as 'candidate-employer1'
, ceh.CompanyName as 'candidate-company1'
, ceh.Title as 'candidate-jobTitle1'
, convert(varchar(10),ceh.StartDate,120) as 'candidate-startDate1'
, convert(varchar(10),ceh.EndDate,120) as 'candidate-endDate1'
, ceh2.CompanyName as 'candidate-employer2'
, ceh2.CompanyName as 'candidate-company2'
, ceh2.Title as 'candidate-jobTitle2'
, convert(varchar(10),ceh2.StartDate,120) as 'candidate-startDate2'
, convert(varchar(10),ceh2.EndDate,120) as 'candidate-endDate2'
, ceh3.CompanyName as 'candidate-employer3'
, ceh3.CompanyName as 'candidate-company3'
, ceh3.Title as 'candidate-jobTitle3'
, convert(varchar(10),ceh3.StartDate,120) as 'candidate-startDate3'
, convert(varchar(10),ceh3.EndDate,120) as 'candidate-endDate3'
, cwh.CandidateWorkHistory as 'candidate-workHistory'
, cdf.CandidateDocument as 'candidate-resume'
--NOTES
, concat('Candidate Mindscope ID: ',c.CandidateID,char(10)
	, iif(c.CandidateStatusID = 0 or c.CandidateStatusID is NULL,'',concat('*Candidate status: ',cs.Description,char(10)))
	, iif(c.CandidateRatingID = 0 or c.CandidateRatingID is NULL,'',concat('*Spoken English: ',cr.Description,char(10)))
	, iif(c.MarketingSourceID = 0 or c.MarketingSourceID is NULL,'',concat('*Source: ',ms.Description,char(10)))
	, iif(c.DivisionID = 0 or c.DivisionID is NULL,'',concat('*Division: ',d.Description,char(10)))
	, iif(c.DepartmentID = 0 or c.DepartmentID is NULL,'',concat('*Department: ',de.Description,char(10)))
	, iif(c.OfficeID = 0 or c.OfficeID is NULL,'',concat('*Location: ',o.Description,char(10)))
	, iif(c.CandProfileNoteID = 0 or c.CandProfileNoteID is NULL,'',concat('*Candidate Profile Note: ',Note.NoteText,char(10)))
	, iif(c.LastContactDate is NULL,'',concat('*Last contacted: ',convert(varchar(10),c.LastContactDate,120),char(10)))
	, iif(c.IndeedID = '' or c.IndeedID is NULL,'',concat('*Indeed ID: ',c.IndeedID,char(10)))
	, iif(c.CandidateLatestInformationID = 0 or c.CandidateLatestInformationID is NULL,''
		,concat('*Latest information Info: ',char(10),'Total compensation: ',cli.TotalCompensation,char(10)))
	, iif(cf1.FieldValue = '' or cf1.FieldValue is NULL,'',concat('*Technical / Domain Experience (Minimum 5 words and 30 characters): ',cf1.FieldValue,char(10)))
	, iif(cf2.FieldValue = '' or cf2.FieldValue is NULL,'',concat('*Current Salary: ',cf2.FieldValue,char(10)))
	, iif(cf3.FieldValue = '' or cf3.FieldValue is NULL,'',concat('*Expected Salary: ',cf3.FieldValue,char(10)))
	, iif(cf4.FieldValue = '' or cf4.FieldValue is NULL,'',concat('*Outlook: ',cte.Description,char(10)))
	, iif(cf5.FieldValue = '' or cf5.FieldValue is NULL,'',concat('*Presentation: ',cte2.Description,char(10)))
	, iif(cf6.FieldValue = '' or cf6.FieldValue is NULL,'',concat('*Push /Pull Factors (Minimum 5 words and 30 characters): ',cf6.FieldValue,char(10)))
	, iif(cf7.FieldValue = '' or cf7.FieldValue is NULL,'',concat('*Recent Interviews: ',cf7.FieldValue))
	) as 'candidate-note'
, cnf.CandidateNoteFinal as 'candidate-comment'
from Candidate c
left join Person p on c.PersonID = p.PersonID
left join Address a on a.AddressID = p.DefaultAddressID
left join Salutation s on s.SalutationID = p.SalutationID and s.Deleted = 0
left join CandidateSecureInformation csi on csi.CandidateSecureInformationID = c.CandidateSecureInformationID
left join CandidateOwners co on co.CandidateID = c.CandidateID
left join Division d on d.DivisionID = c.DivisionID
left join Department de on de.DepartmentID = c.DepartmentID
left join Office o on o.OfficeID = c.OfficeID
left join DefaultCandidateAddress dca on dca.CandidateID = c.CandidateID
left join CandidateStatus cs on cs.CandidateStatusID = c.CandidateStatusID
left join MarketingSource ms on ms.MarketingSourceID = c.MarketingSourceID
left join CandidateLatestInformation cli on cli.CandidateLatestInformationID = c.CandidateLatestInformationID
left join Note on Note.NoteID = c.CandProfileNoteID
left join PersonSplitEmails2Dup psm on psm.CandidateID = c.CandidateID and psm.rn = 1
----left join CandidateEmail ce on ce.CandidateID = c.CandidateID | without mail splitting
left join CandidateRating cr on cr.CandidateRatingID = c.CandidateRatingID
left join (select * from CandidateEmpHistory where rn=1) ceh on ceh.CandidateID = c.CandidateID
left join (select * from CandidateEmpHistory where rn=2) ceh2 on ceh2.CandidateID = c.CandidateID
left join (select * from CandidateEmpHistory where rn=3) ceh3 on ceh3.CandidateID = c.CandidateID
left join CandidateWorkHistory cwh on cwh.CandidateID = c.CandidateID
left join (select * from CustomField where ApplicationLabelID = 1366) cf1 on cf1.ParentID = c.CandidateID --> Technical / Domain Experience (Minimum 5 words and 30 characters)
left join (select * from CustomField where ApplicationLabelID = 1367) cf2 on cf2.ParentID = c.CandidateID --> Current Salary
left join (select * from CustomField where ApplicationLabelID = 1368) cf3 on cf3.ParentID = c.CandidateID --> Expected Salary
left join (select * from CustomField where ApplicationLabelID = 1369) cf4 on cf4.ParentID = c.CandidateID --> Outlook
left join (select * from CustomTableEntry where CustomTableID = 1001) cte on cte.CustomTableEntryID = cf4.FieldValue --> CustomTableID = 1001 for Outlook from CustomTable
left join (select * from CustomField where ApplicationLabelID = 1501) cf5 on cf5.ParentID = c.CandidateID --> Presentation
left join (select * from CustomTableEntry where CustomTableID = 1002) cte2 on cte2.CustomTableEntryID = cf5.FieldValue --> CustomTableID = 1002 for Presentation from CustomTable
left join (select * from CustomField where ApplicationLabelID = 1502) cf6 on cf6.ParentID = c.CandidateID --> Push /Pull Factors (Minimum 5 words and 30 characters)
left join (select * from CustomField where ApplicationLabelID = 1503) cf7 on cf7.ParentID = c.CandidateID --> Recent Interviews
left join CandidateCellPhone ccp on ccp.CandidateID = c.CandidateID
left join CandidatePhone cp on cp.CandidateID = c.CandidateID
left join CandidateDocumentFinal cdf on cdf.CandidateID = c.CandidateID
left join CandidateNoteFinal cnf on cnf.CandidateID = c.CandidateID
where c.Deleted = 0
--and c.CandidateID = 16187

--PART 2:
-----INSERT CANDIDATE COMMENTS
with PersonSchedules as (select PS.PersonID, PS.ScheduleID, PS.EnteredByID, PS.EnteredDate, S.StartTime
	, P.FirstName as EnteredFirstName, P.LastName as EnteredLastName, S.ScheduleTypeID, ST.Description as ScheduleType
	, S.ScheduleStatusID, SS.Description as ScheduleStatus, S.Subject as ScheduleSubject, S.NoteID
	, Note.NoteText, PS.ModifiedDate
	from Person_Schedule PS
	left join Recruiter R on R.RecruiterID = PS.EnteredByID --> Recruiter person should be the person who entered the schedule
	left join Person P on P.PersonID = R.PersonID
	left join Schedule S on S.ScheduleID = PS.ScheduleID
	left join ScheduleType ST on ST.ScheduleTypeID = S.ScheduleTypeID
	left join ScheduleStatus SS on SS.ScheduleStatusID = S.ScheduleStatusID
	left join Note on Note.NoteID = S.NoteID
	where P.Deleted = 0 and S.Deleted = 0)

, CandidateSchedule_Note as (select C.CandidateID, C.PersonID, PS.NoteID, PS.StartTime
	, PS.ModifiedDate, PS.EnteredByID, PS.EnteredFirstName as EnteredFirstName, PS.EnteredLastName as EnteredLastName
	, concat(coalesce('Entered By: ' + PS.EnteredFirstName + ' ' + PS.EnteredLastName + char(10),'')
	, coalesce('Note created: ' + convert(varchar(20),PS.StartTime,120) + char(10),'')
	, coalesce('Last Modified: ' + convert(varchar(20),PS.ModifiedDate,120) + char(10),'')
	, coalesce('Schedule Type: ' + PS.ScheduleType + char(10),'')
	, coalesce('Schedule Subject: ' + PS.ScheduleSubject + char(10),'')
	, 'Notes: ',cast(PS.NoteText as nvarchar(max))) as 'MSC_comment_body' --> this is for candidate schedule
from Candidate C
left join PersonSchedules PS on PS.PersonID = C.PersonID
where C.Deleted = 0 --> 'and PS.NoteText is not NULL' to be removed

UNION ALL

select C.CandidateID, PN.PersonID, PN.NoteID, PN.EnteredDate
	, PN.ModifiedDate, PN.EnteredByID, P.FirstName as EnteredFirstName, P.LastName as EnteredLastName
	, concat(coalesce('Entered By: ' + P.FirstName + ' ' + P.LastName + char(10),'')
	, coalesce('Note created: ' + convert(varchar(20),PN.EnteredDate,120) + char(10),'')
	, coalesce('Last Modified: ' + convert(varchar(20),PN.ModifiedDate,120) + char(10),'')
	, coalesce('Note Type: ' + NT.Description + char(10),'')
	, 'Notes: ',cast(NoteText as nvarchar(max))) as 'MSC_comment_body' --> this is for candidate notes
from Candidate C
left join Person_Note PN on PN.PersonID = C.PersonID
left join Note on Note.NoteID = PN.NoteID
left join NoteType NT on NT.NoteTypeID = Note.NoteTypeID
left join Recruiter R on R.RecruiterID = C.EnteredByID --> v2.RecruiterID instead of R.PersonID
left join Person P on P.PersonID = R.PersonID
where Note.Deleted = 0 and P.Deleted = 0)

select concat('MSC',CandidateID) as 'MSC_candidateID'
	, PersonID
	, -10 as 'MSC_user_account_id'
	, StartTime as 'MSC_feedback_timestamp'
	, MSC_comment_body
	, 0 as 'MSC_feedback_score'
	, StartTime as 'MSC_insert_timestamp'
	, 4 as 'MSC_contact_method'
	, 1 as 'MSC_related_status'
from CandidateSchedule_Note
where CandidateID = 6095
order by CandidateID, StartTime desc