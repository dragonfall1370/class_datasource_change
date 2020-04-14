with pa_candidate_owner as (select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.candidate_owner_json::jsonb) as pa_candidate_owner
	, c.candidate_owner_json
	, 2 owner_index
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and c.candidate_owner_json is not NULL
	--and m.vc_candidate_id = 42215
	--and m.vc_pa_candidate_id = 195802
	
UNION ALL
select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.candidate_owner_json::jsonb) as vc_candidate_owner
	, c.candidate_owner_json
	, 1 owner_index
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_candidate_id = c.id
	where 1=1
	and c.candidate_owner_json is not NULL
	--and m.vc_candidate_id = 42215
)

, merged_new as (select vc_candidate_id
	, array_to_json(array_agg(distinct pa_candidate_owner)) as new_candidate_owner
	from pa_candidate_owner
	where pa_candidate_owner is not NULL
	group by vc_candidate_id)
	
	--select * from merged_new --9541 rows
  --where vc_candidate_id = 42215
	
--MAIN SCRIPT
update candidate c
set candidate_owner_json = m.new_candidate_owner
from merged_new m
where m.vc_candidate_id = c.id
--and m.vc_candidate_id = 42215