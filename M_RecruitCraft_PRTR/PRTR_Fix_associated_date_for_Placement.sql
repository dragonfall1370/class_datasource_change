/* CORRECT ASSOCIATED DATE FOR PLACED */

select pc.id, pc.position_description_id, pc.candidate_id, pc.associated_date
, c.external_id as CandidateExtID
, pd.external_id as JobExtID
, pc.status
from position_candidate pc
left join candidate c on pc.candidate_id = c.id
left join position_description pd on pc.position_description_id = pd.id
where pc.associated_date > now() --54 rows --Those apps have associated_date greater than now()

