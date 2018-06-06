with JobAppStatus as (select JA.ContactID, JA.FullName, JA.JobOrderID, JA.JobTitle
, JA.StatusID, JST.Name ,JST.StageID
from JobApplication JA
left join JobApplicationStatus JST on JST.StatusID = JA.StatusID)

select concat('IH',JA.ContactID) as 'application-candidateExternalId'
, JA.FullName
, concat('IH',JA.JobOrderID) as 'application-positionExternalId'
, JA.JobTitle
, JA.StageID
, case 
when JAS.Name = 'New' then 'SHORTLISTED'
when JAS.Name = 'Internal Interview' then 'SHORTLISTED'
when JAS.Name = 'Submitted' then 'SHORTLISTED'
when JAS.Name = 'Client Interview' then 'SENT'
when JAS.Name = 'References' then '1ST_INTERVIEW'
when JAS.Name = 'Offers' then 'OFFERED'
else 'SHORTLISTED' end as 'application-stage'
from JobAppStatus JA
left join JobApplicationStage JAS on JAS.StageID = JA.StageID