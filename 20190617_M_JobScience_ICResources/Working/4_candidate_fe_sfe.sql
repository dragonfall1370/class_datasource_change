--CANDIDATE POSITION as FE
with position_cand as (select id
			, position_c
			, recordtypeid
			, trim(unnest(string_to_array(position_c, ';'))) as position_cand
			from contact)

--select * from position_cand
--where id = '0030Y00000j0tVMQAY'

, candposition as (select id as candidate_id
			, position_c as original
			, 'POSITION' as vcfe
			, case position_cand
			when 'Tech Author' then 'Tech Author'
			when 'Mid-level' then 'Staff'
			when 'HR' then 'HR'
			when 'Team Leadership' then 'Staff'
			when 'Management' then 'Management'
			when 'Finance' then 'Finance'
			when 'Project / Programme' then 'Project / Programme'
			when 'CXO' then 'CXO'
			--when 'Support' then ''
			--when 'Regional Management' then ''
			when 'Senior/Princ' then 'Staff'
			when 'Architect' then 'Architect'
			when 'Director / VP' then 'Director / VP'
			when 'Grad/Junior' then 'Staff'
			when 'FieldService' then 'FieldService'
			when 'Applications' then 'Applications'
			when 'Technician' then 'Technician'
			else NULL end as vcsfe
			, now() as insert_timestamp
			from position_cand
			where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV') --124420
)

select candidate_id
, original
, vcfe
, vcsfe
, insert_timestamp
from candposition
where 1=1
and vcsfe is not NULL --122783
--and sfe is null --1637
--and candidate_id ='0030Y00000j0tVMQAY'

UNION

--CANDIDATE FE/SFE (skills)
select distinct c.id as candidate_id
		, s.ts2_skill_name_c
		, fe.vcfe
		, fe.vcsfe
		, now() as insert_timestamp
from ts2_skill_c s--349730
left join contact c on s.ts2_contact_c = c.id
left join (select distinct js_category, vcfe, vcsfe from candidate_fe_sfe where note = 'Candidate category') fe on trim(lower(fe.js_category)) = trim(lower(s.ts2_skill_name_c))
where 1=1
and c.recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')  --54117 rows
and s.ts2_skill_name_c is not NULL --53996
--and (fe.vcfe is NULL) --check case in capital letter --147339
and (fe.vcfe is not NULL or fe.vcsfe is not NULL) --145537

--total: 286862