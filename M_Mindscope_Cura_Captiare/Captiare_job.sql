--DUPLICATION REGCONITION
with dup as (SELECT JO.JobOrderID, P.Description as JobTitle, ROW_NUMBER() OVER(PARTITION BY P.Description ORDER BY JO.JobOrderID ASC) AS rn 
FROM JobOrder JO
left join Position P on JO.PositionID = P.PositionID
where JO.Deleted = 0)

--JOB OWNERS
, MaxRecruiterEmailID as (select R.RecruiterID, max(E.EmailID) as MaxEmailID 
	from Recruiter R
	left join Person_Email PE on PE.PersonID = R.PersonID
	left join Email E on E.EmailID = PE.EmailID
	where E.Deleted = 0
	group by R.RecruiterID)

, JobOwners as (select RJ.JobOrderID, RJ.RecruiterID, MRE.MaxEmailID, Email.EmailAddress 
	from Recruiter_JobOrder RJ
	left join MaxRecruiterEmailID MRE on MRE.RecruiterID = RJ.RecruiterID
	left join Email on Email.EmailID = MRE.MaxEmailID
	where RJ.Deleted = 0)

--JOB SCHEDULE
, JobSchedules as (select JS.JobOrderID, JS.ScheduleID, JS.EnteredByID, JS.EnteredDate, S.StartTime
	, P.FirstName as EnteredFirstName, P.LastName as EnteredLastName, S.ScheduleTypeID, ST.Description as ScheduleType
	, S.ScheduleStatusID, SS.Description as ScheduleStatus, S.Subject as ScheduleSubject, S.NoteID, Note.NoteText
	from JobOrder_Schedule JS
	left join Person P on P.PersonID = JS.EnteredByID and P.Deleted = 0
	left join Schedule S on S.ScheduleID = JS.ScheduleID and S.Deleted = 0
	left join ScheduleType ST on ST.ScheduleTypeID = S.ScheduleTypeID
	left join ScheduleStatus SS on SS.ScheduleStatusID = S.ScheduleStatusID
	left join Note on Note.NoteID = S.NoteID and Note.Deleted = 0)

, JobSchedule as (select JO.JobOrderID, JO.ClientID, CL.ClientName as CompanyName
	, JS.EnteredByID, JS.EnteredDate, JS.StartTime, JS.ScheduleTypeID, JS.ScheduleType
	, JS.EnteredFirstName, JS.EnteredLastName, JS.ScheduleStatusID, JS.ScheduleStatus, JS.ScheduleSubject, JS.NoteID, JS.NoteText
	from JobOrder JO
	left join Client CL on JO.ClientID = CL.ClientID and JO.Deleted = 0
	left join JobSchedules JS on JS.JobOrderID = JO.JobOrderID
	where JO.Deleted = 0)

, JobScheduleFinal as (SELECT
     JobOrderID,
     STUFF(
         (SELECT '<hr>' + coalesce('Entered Date: ' + convert(varchar(20),EnteredDate,120) + char(10),'') 
		 + coalesce('Entered By: ' + EnteredFirstName + ' ' + EnteredLastName + char(10),'') + coalesce('Date: ' + convert(varchar(20),StartTime,120) + char(10),'') 
		 + coalesce('Schedule Status: ' + ScheduleStatus + char(10),'') + coalesce('Company name: ' + CompanyName + char(10),'')
		 + coalesce('Schedule Type: ' + ScheduleType + char(10),'') + coalesce('Schedule Subject: ' + ScheduleSubject + char(10),'') 
		 + coalesce('Note: ' + cast(NoteText as nvarchar(max)),'')
          from JobSchedule
          WHERE JobOrderID = a.JobOrderID
		  and EnteredDate is not NULL
		  order by EnteredDate desc --> ModifiedDate is not shown in UI
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 4, '')  AS JobScheduleFinal
FROM JobSchedule as a
where EnteredDate is not NULL
GROUP BY a.JobOrderID)

--JOB SKILLS
, JobSkills as (SELECT
     JobOrderID, 
     STUFF(
         (SELECT ', ' + S.Description
          from JobOrderSkill JOS
		  left join Skill S on S.SkillID = JOS.SkillID
          WHERE JobOrderID = a.JobOrderID
		  and JOS.Deleted = 0
		  order by JOS.JobOrderID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS JobSkills
FROM JobOrderSkill as a
where a.Deleted = 0
GROUP BY a.JobOrderID)

--COMPANY ADDRESSES
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

--MAIN SCRIPTS
select concat('MSC',JO.DefaultContactID) as 'position-contactId'
, concat('MSC',JO.JobOrderID) as 'position-externalId'
, iif(JO.JobOrderID in (select JobOrderID from dup where dup.rn > 1)
	, iif(dup.JobTitle = '' or dup.JobTitle is NULL,concat('No job title-',dup.JobOrderID),concat(dup.JobTitle,'-',dup.JobOrderID))
	, iif(P.Description = '' or P.Description is null,concat('No job title -',JO.JobOrderID),P.Description)) as 'position-title'
, P.Description as '(OriginalJobTitle)'
, JO.PositionID
, JO.NumberOfPosition as 'position-headcount'
, JO.EnteredByID, JO.EnteredDate
, JO.EmploymentTypeID
, case 
	when JO.EmploymentTypeID = 1 then 'PERMANENT' --> Permanent Hire
	when JO.EmploymentTypeID = 2 then 'INTERIM_PROJECT_CONSULTING' --> Secondment Contract
	when JO.EmploymentTypeID = 3 then 'CONTRACT' --> Direct Contract
	else '' end as 'position-type'
, N.NoteText as 'position-internalDescription'
, N2.NoteText as 'position-publicDescription'
, JOW.EmailAddress as 'position-owners'
, convert(varchar(10),S.StartDate,120) as 'position-startDate'
, concat(concat('Job order external ID: ',JO.JobOrderID,char(10))
	, iif(cast(JO.EnteredDate as varchar(max)) = '' or JO.EnteredDate is NULL,'',concat('Entered Date: ',convert(varchar(10),JO.EnteredDate,120),char(10)))
	, iif(JO.EnteredByID is NULL,'',concat('Entered by: ',PS.FirstName,' ',PS.LastName,char(10)))
	, iif(CA.ClientAddresses is NULL,'',concat('Job Address: ',CA.ClientAddresses,char(10)))
	, concat('Remaining positions: ',JO.NumberOfPosition - JO.Filled,char(10))
	, iif(JO.JobOrderStatusID is NULL,'',concat('Job Order Status: ',JOS.Description,char(10)))
	, iif(JO.EmploymentTypeID is NULL,'',concat('Employment Type: ',ET.Description,char(10)))
	, iif(JO.JobLocationID is NULL,'',concat('Job Location : ',JOS.Description,char(10)))
	--, iif(JO.JobOrderLatestInformationI = 0 or JO.JobOrderLatestInformationID is NULL,'',concat('Job Order Latest Information: ',JOLI.NoteText,char(10)))
	, iif(JS.JobSkills is NULL,'',concat('Job Skills: ',JS.JobSkills))
	) as 'position-note'
, iif(JSF.JobScheduleFinal = '' or JSF.JobScheduleFinal is NULL,'',concat('*Activity to date: ',JSF.JobScheduleFinal)) as 'position-comment'
from JobOrder JO
left join Position P on P.PositionID = JO.PositionID
left join Recruiter R on R.RecruiterID = JO.EnteredByID
left join Person PS on PS.PersonID = R.PersonID
left join Schedule S on S.ScheduleID = JO.ScheduleID --> join Schedule table to get info from job order details
left join ClientAddresses CA on CA.ClientID = JO.ClientID
left join EmploymentType ET on ET.EmploymentTypeID = JO.EmploymentTypeID
left join JobOrderStatus JOS on JOS.JobOrderStatusID = JO.JobOrderStatusID
left join JobOwners JOW on JOW.JobOrderID = JO.JobOrderID
left join Note N on N.NoteID = JO.FunctionNoteID
left join Note N2 on N2.NoteID = JO.RequirementNoteID
left join JobSkills JS on JS.JobOrderID = JO.JobOrderID
--left join JobOrderLatestInformation JOLI on JOLI.JobOrderLatestInformationID = JO.JobOrderLatestInformationID --> no need to get the job order latest info
left join JobScheduleFinal JSF on JSF.JobOrderID = JO.JobOrderID
left join dup on dup.JobOrderID = JO.JobOrderID
where JO.Deleted = 0