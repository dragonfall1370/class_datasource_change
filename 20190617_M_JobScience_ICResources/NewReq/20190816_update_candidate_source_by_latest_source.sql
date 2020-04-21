--BACKUP CANDIDATE SOURCE
select id, first_name, last_name, external_id, email
, candidate_source_id
into mike_candidate_source_bkup_20190816
from candidate

--UPDATE NEW CANDIDATE SOURCES
select c.id as cand_ext_id
, c.latest_source_c
, s.name as latest_candidate_source
from contact c
left join ts2_source_c s on s.ts2_source_c_1_id = c.latest_source_c
where c.latest_source_c is not NULL
and c.recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')

--updated 17074 candidate sources