with JobApplication as (select JobId, ApplicantId, max(ApplicantActionId) as maxAppActionId
 from ApplicantActions
 where JobId is not null and ApplicantId is not null
 group by JobId, ApplicantId)

select concat('MP',ja.JobId) as 'application-positionExternalId',
	concat('MP',ja.ApplicantId) as 'application-candidateExternalId',
	case aag.StatusId
		when 29 then 'SENT'
		when 32 then '1ST_INTERVIEW'
		when 33 then '2ND_INTERVIEW'
		when 40 then '2ND_INTERVIEW'
		when 43 then '2ND_INTERVIEW'
		when 34 then 'OFFERED'
		when 41 then 'OFFERED'
		when 35 then 'PLACED'
		else 'SHORTLISTED' END AS 'application-stage'
from JobApplication ja left join VW_APPLICANT_ACTION_GRID aag on ja.maxAppActionId = aag.ApplicantActionId
order by ja.JobId
