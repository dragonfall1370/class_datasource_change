--Candidate work history | VC temp table
create table mike_tmp_work_history_json
(cand_ext_id character varying (100)
, candidate_id bigint
, cand_work_history text)

--Update from VC
update candidate c
set experience_details_json = m.cand_work_history::text
from mike_tmp_work_history_json m
where m.candidate_id = c.id -- rows