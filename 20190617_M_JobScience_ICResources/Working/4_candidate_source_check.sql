-- CANDIDATE SOURCES
select
	c.id as candidate_id
	, firstname
	, lastname
	, trim(c.ts2_source_c) as "candidate-source"
from contact c
where c.recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')
and c.ts2_source_c is NULL --13164 | 13143