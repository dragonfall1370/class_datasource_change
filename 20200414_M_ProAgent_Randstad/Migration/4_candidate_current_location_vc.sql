--Update candidate current location from VC
update candidate c
set current_location_id = cl.id
from common_location cl
where cl.current_location_candidate_id = c.id
and c.external_id is not NULL
and c.deleted_timestamp is NULL