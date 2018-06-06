with CandNote as (select n.ParentId, n.Text, n.CreatedBy, concat(u.FirstName,' ',u.LastName) as CreatedByName, n.CreatedDate 
from Note n
left join Users u on n.CreatedBy = u.Id)

, CandidateNote (ParentId, Note) as (SELECT
     ParentId,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar(10),CreatedDate,120) + ' || ' + 'Created by: ' + CreatedByName + ' || ' + Text
          from  CandNote
          WHERE ParentId = a.ParentId
		  order by CreatedDate desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
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
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM DocumentEdited as a
where Filename not like '%.png%' and Filename not like '%.jpg%'
GROUP BY a.DynamicDataId)


select concat('MCMalaysia',C.DynamicDataId) as 'candidate-externalId'
, C.VCTitle as 'candidate-title'
, coalesce(C.FirstName,'Firstname') as 'candidate-firstName'
, coalesce(rtrim(ltrim(C.LastName)),concat('Lastname-',C.DynamicDataId)) as 'candidate-Lastname'
, convert(varchar(10),C.DateofBirth,120) as 'candidate-dob'
, iif(C.Email= '' or C.Email is NULL,concat('candidate-',C.DynamicDataId,'@monroemalaysia.com'),C.Email) as 'candidate-email'
, C.Email2 as 'candidate-workEmail'
, C.Mobile as 'candidate-phone'
, C.Mobile as 'candidate-mobile'
, C.Mobile2 as 'candidate-workPhone'
, C.HomeTel as 'candidate-homePhone'
, concat(coalesce(C.Add1 + ', ',''),coalesce(C.Add2 + ', ',''),coalesce(C.Town + ', ',''),coalesce(C.HomeProvince + ', ','')
, coalesce(C.Postcode,'')) as 'candidate-address'
, C.Town as 'candidate-city'
, C.Postcode as 'candidate-zipCode'
, C.Nationality
, case C.Nationality
when 'Indonesian' then 'ID'
when 'Malaysian' then 'MY'
when 'Vietnamese' then 'VN'
when 'Singaporean' then 'SG'
when 'Indian' then 'IN'
when 'German' then 'DE'
when 'Thai' then 'TH'
when 'Filipino' then 'PH'
when 'Australian' then 'AU'
when 'Malays' then 'MY'
when 'Bangladeshi' then 'DB'
when 'Philippino' then 'PH'
when 'Punjabi' then 'IN'
when 'American' then 'US'
when 'Phillipino' then 'PH'
when 'Philippino ' then 'PH'
when 'British' then 'GB'
when 'Indo' then 'ID'
when 'Spanish' then 'ES'
when 'Austrian' then 'AT'
when 'Iranian' then 'IR'
when 'French' then 'FR'
when 'Chinese' then 'CN'
when 'Malaysia ' then 'MY'
when 'Malaysa' then 'MY'
when 'Malaysin' then 'MY'
when 'Pakistani' then 'PK'
when 'Malayi' then 'MW'
when 'Maldivan' then 'MV'
when 'Danish' then 'DK'
when 'Swedish' then 'SE'
when 'Kazakhstani' then 'KZ'
when 'Netherlander' then 'NL'
when 'Malaysian ' then 'MY'
when 'Malaysis' then 'MY'
when 'Lithuanian' then 'LT'
when 'Macedonian' then 'MK'
when 'Italian' then 'IT'
when 'Malaysiam' then 'MY'
when 'Dutch' then 'NL'
when 'Sina' then 'IR'
when 'Malaysioa' then 'MY'
when 'Malaysianm' then 'MY'
when 'Malaysia' then 'MY'
when 'Malawian' then 'MW'
when 'Malagasy' then 'MG'
when 'Malaysoa' then 'MY'
when 'Malian' then 'ML'
when 'Singapore ' then 'SG'
when 'Malaysiao' then 'MY'
when 'Luxembourger' then 'LU'
when 'Swiss' then 'CH'
when 'Malayso' then 'MY'
when 'Canadian' then 'CA'
else '' end as 'candidate-citizenship'

, C.CurrentCompany as 'candidate-employer1'
, C.CurrentJobTitle as 'candidate-jobTitle1'
, concat(iif(C.ResponsibilitiesandAchievements = '' or C.ResponsibilitiesandAchievements is NULL,'', concat('Responsibilities & Achievements: ',C.ResponsibilitiesandAchievements,char(10)))
, iif(C.RolesHistory = '' or C.RolesHistory is NULL,'',concat('Roles History:',C.RolesHistory,char(10)))
, iif(C.Started = '' or C.Started is NULL,'',concat('Started from: ',convert(varchar(10),C.Started,110)))) as 'candidate-company1'
, convert(varchar(10),C.Started,120) as 'candidate-startDate1'
, C.Salary as 'candidate-currentSalary'
, concat(coalesce(C.JobFunction + ',',''),coalesce(C.JobFunctions + ',','')) as 'candidate-skills'
, left(C.LinkedIn,254) as 'candidate-linkedIn'
, CF.CanFiles as 'candidate-resume'

, left(concat('Candidate External ID: ',C.DynamicDataId,char(10)
,iif(C.Name = '' or C.Name is NULL,'',concat('Name: ',C.Name,char(10))) 
,iif(C.CreatedDate = '' or C.CreatedDate is NULL,'',concat('Created Date: ',convert(varchar(10),C.CreatedDate,110),char(10)))
,iif(C.Source = '' or C.Source is NULL,'',concat('Source: ',C.Source,char(10)))
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
,iif(C.LastUpdatedDate = '' or C.LastUpdatedDate is NULL,'',concat('Last Updated Date: ',convert(varchar(10),C.LastUpdatedDate,110),char(10)))
,iif(C.LastUpdatedBy = '' or C.LastUpdatedBy is NULL,'',concat('Last Updated By: ',C.LastUpdatedBy))
),32000) as 'candidate-note'
, left(replace(replace(replace(CN.Note,'&lt;','<'),'&gt;','>'),'&amp;','&'),32000) as 'candidate-comments'
from Candidate C
left join CandidateFile CF on C.DynamicDataId = CF.DynamicDataId
left join CandidateNote CN on C.DynamicDataId = CN.ParentId
order by C.DynamicDataId
