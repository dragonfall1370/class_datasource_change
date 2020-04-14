--No of employees | VC temp table
create table mike_tmp_no_employees (
cand_ext_id character varying (100)
, candidate_id bigint
, company_count int
)

--Update from VC
update candidate c
set company_count = m.company_count
from mike_tmp_no_employees m
where m.candidate_id = c.id --166281 rows