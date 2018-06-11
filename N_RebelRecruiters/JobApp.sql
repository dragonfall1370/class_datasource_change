with
temp as (select job_ref, cand_ref, max(event_ref) as maxAppActionId
 from events
 where job_ref <> 0 and cand_ref <> 0 
 group by job_ref, cand_ref)

select concat('REBEL',t.job_ref) as 'application-positionExternalId'
, concat('REBEL',t.cand_ref) as 'application-candidateExternalId'
, case
		when et.event_description in ('CV Submitted') then 'SENT'
		when et.event_description in ('Interview Request','Interview Confirmed') then 'FIRST_INTERVIEW'
		when et.event_description in ('Offer') then 'OFFERED'
		when et.event_description in ('Candidate Closed','Client Satisfied, Job Placed','Candidate Accepted','Candidate No Show','Candidate Did Show') then 'PLACED'
		else 'SHORTLISTED' END AS 'application-stage'
, maxappActionid, et.event_description
from temp t left join events e on t.maxAppActionId = e.event_ref
		left join eventtype et on e.event_type = et.event_type

--select * from temp1 order by job_ref