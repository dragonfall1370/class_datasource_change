--#CF Latest employer | 11324 | Text Area
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11324 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11324) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11324 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
	
--#CF Period: To - year | 11308 | Text Area
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.text_data
	, 11305 parent_id
	, 11308 children_id
	, a.index
	from mike_tmp_candidate_dup_check m
	join (select * from configurable_form_group_value where parent_id = 11305 and children_id = 11308) a on a.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.text_data is not NULL and a.text_data <> ''
	)
	
--IF USING OVERWRITE
insert into configurable_form_group_value (candidate_id, parent_id, children_id, text_data, index)
select vc_candidate_id as candidate_id
, 11305 parent_id
, 11308 children_id
, text_data
, index
from merged_new m
where vc_candidate_id not in (select candidate_id from configurable_form_group_value where children_id = 11308)