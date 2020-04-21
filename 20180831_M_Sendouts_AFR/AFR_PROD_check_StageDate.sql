--REFERENCE SCRIPT TO CHECK SPECIFIC JOB APPLICATION
select c.external_id as CandidateExt
, pd.external_id as JobExt
, pc.id
, pc.position_description_id
, pc.candidate_id
, pc.associated_date
, pc.interview1_date
, pc.interview2_date
, pc.offer_date
, pc.hire_date
from position_candidate pc
left join candidate c on pc.candidate_id = c.id
left join position_description pd on pc.position_description_id = pd.id
where pc.id = 338991

