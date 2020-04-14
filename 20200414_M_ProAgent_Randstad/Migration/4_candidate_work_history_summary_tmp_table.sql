--Work history summary | VC temp table
create table mike_tmp_work_history_summary (
cand_ext_id character varying (100)
, candidate_id bigint
, work_history_summary text
)

--Update from VC
update candidate c
set experience = m.work_history_summary
from mike_tmp_work_history_summary m
where m.candidate_id = c.id --140284 rows