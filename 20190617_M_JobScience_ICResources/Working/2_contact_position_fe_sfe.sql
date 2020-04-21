--Contact position as FE
with position_cand as (select id
			, position_c
			, recordtypeid
			, trim(unnest(string_to_array(position_c, ';'))) as position_cand
			from contact)
			
select id as contact_id
		, 'POSITION' as FE
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
		else NULL end as SFE
		, now() as insert_timestamp
from position_cand
where recordtypeid in ('0120Y0000013O5d') --29112

---