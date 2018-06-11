update temp_candidatecustomfields
set vccandidateid = candidate.id
from candidate
where candidateexternalid = candidate.external_id 
and vccandidateid is null