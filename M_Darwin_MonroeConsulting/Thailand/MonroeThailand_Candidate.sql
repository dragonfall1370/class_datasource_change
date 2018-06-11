with CandNote as (select n.ParentId, n.Text, n.CreatedDate, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, u.Id
from Notes n
left join Users u on n.CreatedBy = u.Id)

, CandidateNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT '<hr>' + 'Created date: ' + convert(varchar(20),CreatedDate,120) + char(10) + 'Created by: ' + CreatedByName + ' || ' + Text
          from  CandNote
          WHERE ParentId = a.ParentId
		  order by CreatedDate desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 4, '')  AS URLList
FROM CandNote as a
GROUP BY a.ParentId)

, DocumentEdited as (select DynamicDataId, replace(Filename,',','') as Filename from Document)

, CandidateFile (DynamicDataId, CanFiles)
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

--CANDIDATE DUPLICATE MAIL REGCONITION
, EmailDupRegconition as (SELECT DynamicDataId, Email, ROW_NUMBER() OVER(PARTITION BY Email ORDER BY DynamicDataId ASC) AS rn 
from Candidate where Email like '%_@_%.__%')

, CandidateEmail as (select DynamicDataId
, case	when rn = 1 then Email
		else concat('duplicate',rn,'-',Email) end as CandidateEmail
, rn
from EmailDupRegconition)

--CANDIDATE WORK EMAIL RECOGNITION
, WorkEmail as (SELECT DynamicDataId, Email2, ROW_NUMBER() OVER(PARTITION BY Email2 ORDER BY DynamicDataId ASC) AS rn 
from Candidate where Email2 like '%_@_%.__%')

, CandidateWorkEmail as (select DynamicDataId
, case	when rn = 1 then Email2
		else concat('duplicate',rn,'-',Email2) end as CandidateEmail
, rn
from WorkEmail)

--MAIN SCRIPT
select concat('MCThailand',C.DynamicDataId) as 'candidate-externalId'
, case when C.Title in ('Mr','Mr.','Mrs','Ms','Ms.','Dr','Miss') then upper(replace(C.Title,'.',''))
else '' end as 'candidate-title'
, C.Title as '(OriginalCandidateTitle)'
, coalesce(C.FirstName,'Firstname') as 'candidate-firstName'
, coalesce(rtrim(ltrim(C.LastName)),concat('Lastname-',C.DynamicDataId)) as 'candidate-Lastname'
, c.Email as '(OriginalEmail)'
, case
	when C.Email is not NULL and C.DynamicDataId in (select DynamicDataId from CandidateEmail) then CE.CandidateEmail
	else concat('candidate_TH-',C.DynamicDataId,'@noemail.com') end as 'candidate-email'
, CWE.CandidateEmail as 'candidate-workEmail'
, convert(varchar(10),C.DateofBirth,120) as 'candidate-dob'
, C.Mobile as 'candidate-phone'
, C.Mobile as 'candidate-mobile'
, C.Mobile2 as 'candidate-workPhone'
, C.HomeTel as 'candidate-homePhone'
, ltrim(stuff((coalesce(', ' + C.Add1,'') + coalesce(', ' + C.Add2,'') + coalesce(', ' + C.Town,'') + coalesce(', ' + C.HomeProvince,'') + coalesce(', ' + C.Postcode,'')),1,2,'')) as 'candidate-address'
, C.Town as 'candidate-city'
, C.Postcode as 'candidate-zipCode'
, C.Nationality
, case
	when C.Nationality like 'Afghan%' then 'AF'
	when C.Nationality like '%Africa%' then 'ZA'
	when C.Nationality like 'Albani%' then 'AL'
	when C.Nationality like 'Americ%' then 'US'
	when C.Nationality like 'Andorr%' then 'AD'
	when C.Nationality like 'Austra%' then 'AU'
	when C.Nationality like 'Austri%' then 'AT'
	when C.Nationality like 'Belgia%' then 'BE'
	when C.Nationality like 'Brazil%' then 'BR'
	when C.Nationality like 'Britis%' then 'GB'
	when C.Nationality like 'Burmes%' then 'MM'
	when C.Nationality like 'Cambod%' then 'KH'
	when C.Nationality like 'Canadi%' then 'ie'
	when C.Nationality like 'Chines%' then 'MO'
	when C.Nationality like 'Dutch%' then 'NL'
	when C.Nationality like 'East%' then 'ZA'
	when C.Nationality like 'Emiria%' then 'AE'
	when C.Nationality like 'Eritre%' then 'ER'
	when C.Nationality like 'Estoni%' then 'EE'
	when C.Nationality like 'Ethiop%' then 'ET'
	when C.Nationality like 'Europe%' then 'TR'
	when C.Nationality like 'Fijian%' then 'FJ'
	when C.Nationality like 'Filipi%' then 'PH'
	when C.Nationality like 'fili%' then 'PH'
	when C.Nationality like 'French%' then 'FR'
	when C.Nationality like 'Gabone%' then 'GA'
	when C.Nationality like 'German%' then 'DE'
	when C.Nationality like 'Ghanai%' then 'GH'
	when C.Nationality like 'Indian%' then 'IN'
	when C.Nationality like 'Indone%' then 'ID'
	when C.Nationality like 'Irania%' then 'IR'
	when C.Nationality like 'Jamaic%' then 'JM'
	when C.Nationality like 'Japane%' then 'JP'
	when C.Nationality like 'Malaga%' then 'MG'
	when C.Nationality like 'Malays%' then 'MY'
	when C.Nationality like 'Namibi%' then 'NA'
	when C.Nationality like 'Nigeri%' then 'NE NG'
	when C.Nationality like 'Pakist%' then 'PK'
	when C.Nationality like 'Philip%' then 'PH'
	when C.Nationality like 'Phili%' then 'PH'
	when C.Nationality like 'Russia%' then 'RU'
	when C.Nationality like 'Singap%' then 'SG'
	when C.Nationality like 'Sri%' then 'LK'
	when C.Nationality like 'Thai%' then 'TH'
	when C.Nationality like 'Vietna%' then 'VN'
	when C.Nationality like '%UNITED%ARAB%' then 'AE'
	when C.Nationality like '%UAE%' then 'AE'
	when C.Nationality like '%U.A.E%' then 'AE'
	when C.Nationality like '%UNITED%KINGDOM%' then 'GB'
	when C.Nationality like '%UNITED%STATES%' then 'US'
	when C.Nationality like '%US%' then 'US'
	else '' end as 'candidate-citizenship'
, concat(coalesce('Current Company: ' + C.CurrentCompany + char(10),''),coalesce('Current JobTitle: ' + C.CurrentJobTitle + char(10),'')
	, coalesce('Current salary: ' + C.Salary + char(10),'')
	, coalesce('Started from: ' + C.Started + char(10),''), coalesce('Responsibilities & Achievements: ' + C.ResponsibilitiesandAchievements + char(10),'')
	, coalesce('Roles History:' + C.RolesHistory + char(10),''),coalesce('Reason for Leaving:' + C.ReasonforLeaving,'')) as 'candidate-workHistory'
, C.CurrentCompany as 'candidate-employer1'
, C.CurrentJobTitle as 'candidate-jobTitle1'
, concat(iif(C.Started = '' or C.Started is NULL,'',concat('Started from: ',C.Started,char(10)))
	, iif(C.ResponsibilitiesandAchievements = '' or C.ResponsibilitiesandAchievements is NULL,'',concat('Responsibilities & Achievements: ',C.ResponsibilitiesandAchievements,char(10)))
	, iif(C.ReasonforLeaving = '' or C.ReasonforLeaving is NULL,'',concat('Reason for Leaving:',C.ReasonforLeaving))) as 'candidate-company1'
, 'THB' as 'candidate-currency'
, ltrim(stuff((coalesce(', ' + C.JobFunction,'') + coalesce(', ' + C.JobFunctions,'')),1,2,'')) as 'candidate-skills'
, left(C.LinkedIn,254) as 'candidate-linkedIn'
, CF.CanFiles as 'candidate-resume'
, left(concat('Candidate External ID: ',C.DynamicDataId,char(10)
	,iif(C.Title = '' or C.Title is NULL,'',concat('Candiadte title: ',C.Title,char(10)))
	,iif(C.Name = '' or C.Name is NULL,'',concat('Name: ',C.Name,char(10))) 
	,iif(C.CreatedDate = '' or C.CreatedDate is NULL,'',concat('Created Date: ',C.CreatedDate,char(10)))
	,iif(C.Email = '' or C.Email is NULL,'',concat('Candidate original email: ',C.Email,char(10)))
	,iif(C.Email2 = '' or C.Email2 is NULL,'',concat('Candidate work email: ',C.Email2,char(10)))
	,iif(C.Source = '' or C.Source is NULL,'',concat('Source: ',C.Source,char(10)))
	,iif(C.LookingFor = '' or C.LookingFor is NULL,'',concat('LookingFor: ',C.LookingFor,char(10)))
	,iif(C.Salary = '' or C.Salary is NULL,'',concat('Current salary: ',C.Salary,char(10)))
	,iif(C.CurrentCompany = '' or C.CurrentCompany is NULL,'',concat('CurrentCompany: ',C.CurrentCompany,char(10)))
	,iif(C.Qualifications = '' or C.Qualifications is NULL,'',concat('Qualifications: ',C.Qualifications,char(10)))
	,iif(C.JobFunction = '' or C.JobFunction is NULL,'',concat('JobFunction: ',C.JobFunction,char(10)))
	,iif(C.JobFunctions = '' or C.JobFunctions is NULL,'',concat('Job Functions 2: ',C.JobFunctions,char(10)))
	,iif(C.IndustrySkills = '' or C.IndustrySkills is NULL,'',concat('Industry Skills: ',C.IndustrySkills,char(10)))
	,iif(C.Industry = '' or C.Industry is NULL,'',concat('Industry: ',C.Industry,char(10)))
	,iif(C.Locations = '' or C.Locations is NULL,'',concat('Locations: ',C.Locations,char(10)))
	,iif(C.InterviewNotes = '' or C.InterviewNotes is NULL,'',concat('Interview Notes: ',C.InterviewNotes,char(10)))
	,iif(C.City = '' or C.City is NULL,'',concat('City: ',C.City,char(10)))
	,iif(C.WorkPermit = '' or C.WorkPermit is NULL,'',concat('Work Permit: ',C.WorkPermit,char(10)))
	,iif(C.FixedTermContract = '' or C.FixedTermContract is NULL,'',concat('Fixed Term Contract: ',C.FixedTermContract,char(10)))
	,iif(C.Contract = '' or C.Contract is NULL,'',concat('Contract: ',C.Contract,char(10)))
	,iif(C.Permanent = '' or C.Permanent is NULL,'',concat('Permanent: ',C.Permanent,char(10)))
	,iif(C.DateofBirth = '' or C.DateofBirth is NULL,'',concat('Date of Birth: ',C.DateofBirth,char(10)))
	,iif(C.LastUpdatedDate = '' or C.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),C.LastUpdatedDate,110),char(10)))
	,iif(C.LastUpdatedByEmail = '' or C.LastUpdatedByEmail is NULL,'',concat('Last Updated By: ',C.LastUpdatedByEmail))
	),32000) as 'candidate-note'
, left(CN.Note,32000) as 'candidate-comments'
from Candidate C
left join CandidateFile CF on C.DynamicDataId = CF.DynamicDataId
left join CandidateNote CN on C.DynamicDataId = CN.ParentId
left join CandidateEmail CE on C.DynamicDataId = CE.DynamicDataId --Candidate email check duplication
left join CandidateWorkEmail CWE on C.DynamicDataId = CWE.DynamicDataId
order by C.DynamicDataId