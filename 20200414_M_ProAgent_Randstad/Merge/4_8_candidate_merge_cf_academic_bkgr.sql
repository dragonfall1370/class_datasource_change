--CHECK
select *
from configurable_form_group_value
where 1=1
and parent_id = 11305
and children_id = 11323
--and candidate_id in (select vc_candidate_id from mike_tmp_candidate_dup_check)
and candidate_id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)
/* Detailed counts
CF11306: 676 rows
CF11307: 910 rows
CF11308: 10047 rows
CF11309: 6053 rows
CF11321: 10154 rows
CF11322: 7617 rows
CF11323: 11173 rows
*/

-->#CF | 11306, 11307, 11308, 11309, 11323 | Text Area
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.text_data
	, 11305 parent_id
	, 11306 children_id
	, a.index
	from mike_tmp_candidate_dup_check m
	join (select * from configurable_form_group_value where parent_id = 11305 and children_id = 11306) a on a.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.text_data is not NULL and a.text_data <> ''
	)
	
--IF USING OVERWRITE
insert into configurable_form_group_value (candidate_id, parent_id, children_id, text_data, index)
select vc_candidate_id as candidate_id
, 11305 parent_id
, 11306 children_id
, text_data
, index
from merged_new m
on conflict ON CONSTRAINT configurable_form_group_value_pkey
	do update
	set text_data = excluded.text_data;


-->#CF | 11321, 11322 | Dropdown
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.value_data
	, 11305 parent_id
	, 11321 children_id
	, a.index
	from mike_tmp_candidate_dup_check m
	join (select * from configurable_form_group_value where parent_id = 11305 and children_id = 11321) a on a.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.value_data is not NULL and a.value_data <> ''
	)
	
--IF USING OVERWRITE
insert into configurable_form_group_value (candidate_id, parent_id, children_id, value_data, index)
select vc_candidate_id as candidate_id
, 11305 parent_id
, 11321 children_id
, value_data
, index
from merged_new m
on conflict ON CONSTRAINT configurable_form_group_value_pkey
	do update
	set value_data = excluded.value_data;