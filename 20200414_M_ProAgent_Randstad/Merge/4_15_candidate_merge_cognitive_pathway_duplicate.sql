select *
from additional_form_values
where field_id = 11302
and field_value = '66' --12133

select count(*)
from additional_form_values
where field_id = 11302
and field_value = '66'

select *
into mike_bkup_additional_form_values_20200709
from additional_form_values

--#CF | 11302 | Cognitive Pathway > "Duplicate" for all master records
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11302 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11302) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11302 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
--DOUBLE CHECK: "Duplicate" Cognitive Pathway for current candidates / merged candidates
select *
from additional_form_values
where field_id = 11302
and additional_id in (select vc_candidate_id from mike_tmp_candidate_dup_check)--12111

select *
from additional_form_values
where field_id = 11302
and additional_id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)
and field_value = '66' --Duplicate --12133

select distinct vc_pa_candidate_id
from mike_tmp_candidate_dup_check --12133



--Remove "Duplicate" Cognitive Pathway for PA candidates
delete from additional_form_values
where field_id = 11302
and additional_id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)
and field_value = '66' --Duplicate --12133