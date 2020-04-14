with pa_candidate_workhistory as
(--Current VC work history
select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.experience_details_json::jsonb) as vc_workhistory
	, c.experience_details_json
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_candidate_id = c.id
	where 1=1
	and c.experience_details_json is not NULL and trim(c.experience_details_json) <> '[]'
	and trim(c.experience_details_json) <> ''
	--and substring(c.experience_details_json, length(c.experience_details_json)) = ']' --for audit check
	--and m.vc_candidate_id = 41141
	--and m.vc_pa_candidate_id = 136920
--24055 rows

UNION ALL
--PA work history
select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.experience_details_json::jsonb) as pa_candidate_workhistory
	, c.experience_details_json
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and c.experience_details_json is not NULL
	--and m.vc_candidate_id = 41141
	--and m.vc_pa_candidate_id = 195515	
) --46979 rows

, merged_new as (select vc_candidate_id
	, array_to_json(array_agg(distinct vc_workhistory)) as new_candidate_workhistory
	from pa_candidate_workhistory
	where vc_workhistory is not NULL
	group by vc_candidate_id)

/* AUDIT BEFORE MERGED
select id, experience_details_json
from candidate
where id = 41141
*/

--select * from merged_new -- rows
--where vc_candidate_id = 41141

--MAIN SCRIPT
update candidate c
set experience_details_json = m.new_candidate_workhistory
from merged_new m
where m.vc_candidate_id = c.id
--and m.vc_candidate_id = 41141