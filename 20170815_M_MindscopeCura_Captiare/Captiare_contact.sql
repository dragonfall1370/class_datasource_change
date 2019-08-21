--CONTACT PHONE >> As Contact has no columns for Phone, get Client phones as contact phones
with Phones as (select CC.ContactID, CC.ClientID, Phone.PhoneID, Phone.PhoneTypeID, PhoneType.Description, Phone.PhoneNumber
	from Client_Contact CC									--> main table for contact
	left join Client C on C.ClientID = CC.ClientID			--> join to get phoneID from Client table
	left join Client_Phone CP on CP.ClientID = C.ClientID
	left join Phone on Phone.PhoneID = CP.PhoneID
	left join PhoneType on PhoneType.PhoneTypeID = Phone.PhoneTypeID
	where CC.Deleted = 0 and Phone.PhoneTypeID <> 4)		--> not including Fax (typeID: 4)

, ContactPhone as (SELECT
     ContactID, 
     STUFF(
         (SELECT ', ' + Description + ': ' + PhoneNumber
          from  Phones
          WHERE ContactID = a.ContactID
		  and PhoneID is not NULL
		  order by PhoneTypeID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS ContactPhone
FROM Phones as a
where PhoneID is not NULL
GROUP BY a.ContactID)

--CONTACT OWNERS
, MaxRecruiterEmailID as (select R.RecruiterID, max(E.EmailID) as MaxEmailID 
	from Recruiter R
	left join Person_Email PE on PE.PersonID = R.PersonID
	left join Email E on E.EmailID = PE.EmailID
	where E.Deleted = 0
	group by R.RecruiterID)

, ContactOwners as (select RC.ContactID, RC.RecruiterID, MRE.MaxEmailID, Email.EmailAddress 
	from Recruiter_Contact RC
	left join MaxRecruiterEmailID MRE on MRE.RecruiterID = RC.RecruiterID
	left join Email on Email.EmailID = MRE.MaxEmailID
	where RC.Deleted = 0)

--CONTACT EMAILS
, PersonEmails as (select C.ContactID, PE.PersonID, PE.EmailID, rtrim(ltrim(replace(replace(replace(Email.EmailAddress,' /',','),' ;',','),':',','))) as EmailAddress --> to remove special characters
	from Contact C
	left join Person_Email PE on PE.PersonID = C.PersonID
	left join Email on Email.EmailID = PE.EmailID
	where Email.Deleted = 0 and EmailAddress like '%_@_%.__%')
--select PersonID, count(PersonID) from PersonEmails --> to check if 1 person may have multiple emails
--group by PersonID
--having count(PersonID) > 1
--select * from PersonEmails where PersonID in (14465,14581,16006,16093)

, maildup as (select ContactID, PersonID, EmailAddress, ROW_NUMBER() OVER(PARTITION BY EmailAddress ORDER BY PersonID ASC) AS rn 
	from PersonEmails)
--select PersonID, count(personID) from maildup
--group by PersonID
--having count(personID) > 1
--select * from maildup where PersonID in (14465,14581,16006,16093)

, ContactEmail as (select ContactID, PersonID
	, case when rn = 1 then EmailAddress
	else concat(rn,'-',EmailAddress) end as EmailAddress
	, rn
	from maildup)

, ContactEmailFinal as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + EmailAddress
          from  ContactEmail
          WHERE ContactID = a.ContactID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ContactEmails
FROM ContactEmail as a
GROUP BY a.ContactID)

--select * from ContactEmailFinal where ContactID in (252,650,689,150) | those contacts have many emails
--CONTACT SCHEDULE
, PersonSchedules as (select PS.PersonID, PS.ScheduleID, PS.EnteredByID, Note.EnteredDate, S.StartTime
	, P.FirstName as EnteredFirstName, P.LastName as EnteredLastName, S.ScheduleTypeID, ST.Description as ScheduleType
	, S.ScheduleStatusID, SS.Description as ScheduleStatus, S.Subject as ScheduleSubject, S.NoteID, Note.NoteText, Note.ModifiedDate
	from Person_Schedule PS
	left join Recruiter R on R.RecruiterID = PS.EnteredByID --> Recruiter person should be the person who entered the schedule
	left join Person P on P.PersonID = R.PersonID
	left join Schedule S on S.ScheduleID = PS.ScheduleID
	left join ScheduleType ST on ST.ScheduleTypeID = S.ScheduleTypeID
	left join ScheduleStatus SS on SS.ScheduleStatusID = S.ScheduleStatusID
	left join Note on Note.NoteID = S.NoteID
	where P.Deleted = 0 and S.Deleted = 0 and Note.Deleted = 0)

, ContactSchedule as (select C.ContactID, C.PersonID, C.ClientID, CL.ClientName as CompanyName
	, PS.PersonID as ScPersonID, PS.ScheduleID, PS.EnteredByID, PS.EnteredDate, PS.StartTime, PS.ModifiedDate
	, PS.ScheduleTypeID, PS.ScheduleType, PS.EnteredFirstName, PS.EnteredLastName
	, PS.ScheduleStatusID, PS.ScheduleStatus, PS.ScheduleSubject, PS.NoteID, PS.NoteText
	from Contact C
	left join Client CL on C.ClientID = CL.ClientID
	left join PersonSchedules PS on PS.PersonID = C.PersonID
	where C.Deleted = 0 and CL.Deleted = 0)

, ContactScheduleFinal as (SELECT
     ContactID,
     STUFF(
         (SELECT '<hr>' + coalesce('Entered Date: ' + convert(varchar(20),EnteredDate,120) + char(10),'') 
		 + coalesce('Entered By: ' + EnteredFirstName + ' ' + EnteredLastName + char(10),'')
		 + coalesce('Date: ' + convert(varchar(20),StartTime,120) + char(10),'') 
		 + coalesce('Status: ' + ScheduleStatus + char(10),'') + coalesce('Company name: ' + CompanyName + char(10),'')
		 + coalesce('Schedule type: ' + ScheduleType + char(10),'') + coalesce('Schedule subject: ' + ScheduleSubject + char(10),'') 
		 + coalesce('Note: ' + cast(NoteText as nvarchar(max)),'')
          from ContactSchedule
          WHERE ContactID = a.ContactID
		  and EnteredDate is not NULL
		  order by ModifiedDate desc --> ModifiedDate is not shown in UI | Added for the version 2
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 4, '')  AS ContactScheduleFinal
FROM ContactSchedule as a
where EnteredDate is not NULL
GROUP BY a.ContactID)

--CONTACT NOTES
, PersonNotes as (select C.ContactID, PN.PersonID, PN.NoteID, Note.NoteTypeID, NT.Description, Note.EnteredDate
	, PN.ModifiedDate, PN.EnteredByID, P.FirstName as EnteredFirstName, P.LastName as EnteredLastName, Note.NoteText
	from Contact C
	left join Person_Note PN on PN.PersonID = C.PersonID
	left join Note on Note.NoteID = PN.NoteID
	left join NoteType NT on NT.NoteTypeID = Note.NoteTypeID
	left join Recruiter R on R.RecruiterID = C.EnteredByID  --> v2.edit the recruiterID instead of PersonID
	left join Person P on P.PersonID = R.PersonID
	where Note.Deleted = 0 and P.Deleted = 0)
	--and Note.NoteTypeID = 300) >>> NoteTypeID = 300 for Contact

, ContactNoteFinal as (SELECT
     ContactID,
     STUFF(
         (SELECT char(10) + coalesce('Note created: ' + convert(varchar(20),EnteredDate,120) + char(10),'') + coalesce('Entered By: ' + EnteredFirstName + ' ' + EnteredLastName + char(10),'')
			+ coalesce('Modified Date: ' + convert(varchar(20),ModifiedDate,120) + char(10),'') + coalesce('Note: ' + cast(NoteText as nvarchar(max)),'')
          from PersonNotes
          WHERE ContactID = a.ContactID
		  and EnteredDate is not NULL
		  order by ModifiedDate desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ContactNoteFinal
FROM PersonNotes as a
where EnteredDate is not NULL
GROUP BY a.ContactID)

--CONTACT DOCUMENTS
, Documents as (select PD.PersonID, PD.DocumentID, D.DocumentFileName, D.DocumentTypeID, DT.Description
	from Person_Document PD
	left join Document D on D.DocumentID = PD.DocumentID
	left join DocumentType DT on DT.DocumentTypeID = D.DocumentTypeID
	where D.DocumentFileName like '%.pdf' or D.DocumentFileName like '%.rtf' or D.DocumentFileName like '%.doc%' 
	or D.DocumentFileName like '%.xls%' or D.DocumentFileName like '%.html')

, PersonDocuments as (SELECT
     PersonID,
     STUFF(
         (SELECT ', ' + DocumentFileName
          from  Documents
          WHERE PersonID = a.PersonID
		  order by DocumentTypeID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS PersonDocuments
FROM Documents as a
GROUP BY a.PersonID)

, ContactDocumentFinal as (select C.ContactID, PD.PersonDocuments as ContactDocument
	from Contact C
	left join PersonDocuments PD on PD.PersonID = C.PersonID
	where C.Deleted = 0 and PD.PersonDocuments is not NULL)

--COMPANY ADDRESSES -> Contacts get company adresses as contact's addresses
, Addresses as (select CA.ClientID, CA.AddressID, A.AddressTypeID, AT.Description as AddressType, A.Street, A.Street2, A.CityId, City.Description as City
	, A.ProvinceStateId, PS.Description as Province, A.PostalZip, A.CountryId, CT.Description as Country
	from Client_Address CA
	left join Address A on A.AddressID = CA.AddressID
	left join AddressType AT on AT.AddressTypeID = A.AddressTypeID
	left join City on City.CityID = A.CityId
	left join ProvinceState PS on PS.ProvinceStateID = A.ProvinceStateId
	left join Country CT on CT.CountryId = A.CountryId
	where CA.Deleted = 0) --> 1 Client Address was deleted

, ClientAddresses as (SELECT
     ClientID,
     STUFF(
         (SELECT char(10) + coalesce(AddressType + ': ','') + 
		 stuff((coalesce(', ' + Street,'') + coalesce(', ' + Street2,'') + coalesce(', ' + City,'') + coalesce(', ' + Province,'') 
		 + coalesce(', ' + PostalZip,'') + coalesce(', ' + Country,'')),1,2,'')
          from  Addresses
          WHERE ClientID = a.ClientID
		  order by AddressTypeID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ClientAddresses
FROM Addresses as a
GROUP BY a.ClientID)

--MAIN SCRIPT
select iif(C.ClientID = 0 or C.ClientID is NULL,'MSC9999999',concat('MSC',C.ClientID)) as 'contact-companyId'
, concat('MSC',C.ContactID) as 'contact-externalId'
, iif(P.FirstName = '' or P.FirstName is NULL,'Firstname',P.FirstName) as 'contact-firstName'
, iif(P.LastName = '' or P.LastName is NULL,concat('LastName-',C.ContactID),P.LastName) as 'contact-lastName'
--, P.MiddleName as 'contact-middleName' --> has no middle Name
, C.Title as 'contact-jobTitle'
, CO.EmailAddress as 'contact-owners'
, CP.ContactPhone as 'contact-phone'
, CE.ContactEmails as 'contact-email'
, CDF.ContactDocument as 'contact-document'
, concat(concat('*Contact external ID: ',C.ContactID,char(10))
	, iif(C.ContactStatusID = 0 or C.ContactStatusID is NULL,'',concat('*Contact status: ',CS.Description,char(10)))
	, iif(C.CommunicationMethodID = 0 or C.CommunicationMethodID is NULL,'',concat('*Communication Method: ',CM.Description,char(10)))
	, iif(cast(C.LastContactDate as varchar(max)) = '' or C.CommunicationMethodID is NULL,'',concat('*Last Contact Date: ',convert(varchar(10),C.LastContactDate,120),char(10)))
	, iif(C.DivisionID = 0 or C.DivisionID is NULL,'',concat('*Division: ',D.Description,char(10)))
	, iif(C.DepartmentID = 0 or C.DepartmentID is NULL,'',concat('*Department: ',DE.Description,char(10)))
	, iif(C.OfficeID = 0 or C.OfficeID is NULL,'',concat('*Location: ',O.Description,char(10)))
	, iif(CL.ClientAddresses = '' or CL.ClientAddresses is NULL,'',concat('*Client Addresses: ',CL.ClientAddresses,char(10)))
	, iif(CNF.ContactNoteFinal = '' or CNF.ContactNoteFinal is NULL,'',concat('*Contact notes: ',CNF.ContactNoteFinal))
	) as 'contact-note'
, iif(CSF.ContactScheduleFinal = '' or CSF.ContactScheduleFinal is NULL,'',concat('*Activity to date: ',CSF.ContactScheduleFinal)) as 'contact-comment'
from Contact C
left join Person P on P.PersonID = C.PersonID
left join ContactOwners CO on CO.ContactID = C.ContactID
left join Division D on D.DivisionID = C.DivisionID
left join Department DE on DE.DepartmentID = C.DepartmentID
left join Office O on O.OfficeID = C.OfficeID
left join ContactStatus CS on CS.ContactStatusID = C.ContactStatusID
left join CommunicationMethod CM on CM.CommunicationMethodID = C.CommunicationMethodID
left join ContactPhone CP on CP.ContactID = C.ContactID
left join ContactEmailFinal CE on CE.ContactID = C.ContactID
left join ContactScheduleFinal CSF on CSF.ContactID = C.ContactID
left join ContactNoteFinal CNF on CNF.ContactID = C.ContactID
left join ContactDocumentFinal CDF on CDF.ContactID = C.ContactID
left join ClientAddresses CL on CL.ClientID = C.ClientID and CL.ClientAddresses is not NULL
where C.Deleted = 0 and C.ClientID in (select ClientID from Client where Deleted = 0)

UNION ALL

select 'MSC9999999','MSC9999999','Default','Contact','','','','','','This is default contact from Data Import',''

--DRAFT QUERY
select * from Contact where Deleted = 0

select * from Person where MiddleName is not NULL

select distinct DivisionID from Contact where ContactID = 625 | 15918

select * from Person where PersonID = 15918

select * from Salutation

select * from Division

select * from Client_Address

select * from Address

select * from Office

--TEST RESULTS FOR CONTACT >>862<<
select distinct ContactID from ContactSchedule where ScheduleType is not NULL

select * from ContactSchedule where PersonID = 16947;

select ContactID, count(ContactID) from ContactSchedule group by ContactID --> 862 ContactID has 5 schedules

select * from Client where ClientID = 405

select * from Contact where ContactID = 862 --> 16947 PersonID	405 ClientID

select * from Person_Schedule where PersonID = 16947

select * from Schedule where ScheduleID in (10916,10930,10984,10985,10986)

select * from Note where NoteID in (64489,64557,64817,64818,64819)

select * from Person where PersonID = 16947

select * from Contact where ClientID = 405

--
select PS.*, P.FirstName, P.LastName from Person_Schedule PS
left join Person P on P.PersonID = PS.PersonID
where PS.ScheduleID in (3187,3189,3189,7396)

select * from Schedule where ScheduleID in (3187,3189,3189,7396)