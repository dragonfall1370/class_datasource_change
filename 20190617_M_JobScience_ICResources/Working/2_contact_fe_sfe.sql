--CONTACT FE
select distinct c.id as contact_id
, s.ts2_skill_name_c
, fe.vcfe
, fe.vcsfe
, CURRENT_TIMESTAMP as insert_timestamp
from ts2_skill_c s--349730
left join contact c on s.ts2_contact_c = c.id
left join (select distinct js_category, vcfe, vcsfe from candidate_fe_sfe where note = 'Candidate category') fe on trim(lower(fe.js_category)) = trim(lower(s.ts2_skill_name_c))
where 1=1
and c.recordtypeid in ('0120Y0000013O5d') --54117 rows
and s.ts2_skill_name_c is not NULL --53996
--and (fe.vcfe is NULL) --check case in capital letter --13869
and (fe.vcfe is not NULL or fe.vcsfe is not NULL) --43429