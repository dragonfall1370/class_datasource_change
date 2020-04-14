--Language | VC temp table
create table mike_tmp_language_json (
cand_ext_id character varying (100)
, candidate_id bigint
, language_json character varying (2000)
)

--Update from VC
update candidate c
set skill_details_json = m.language_json::text
from mike_tmp_language_json m
where m.candidate_id = c.id --166281 rows