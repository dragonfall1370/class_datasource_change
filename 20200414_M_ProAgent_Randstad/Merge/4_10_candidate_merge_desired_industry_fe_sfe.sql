-->>MERGED INDUSTRY<<--
with pa_candidate_ind as (select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	--, c.experience_details_json
	, jsonb_array_elements(c.desired_industry_json::jsonb) as pa_candidate_ind
	, c.desired_industry_json
	--, c.desired_industry_json::json->0->>'desiredIndustryId'
	--, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as xyz
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and c.desired_industry_json is not NULL
	--and m.vc_candidate_id = 42215
	--and m.vc_pa_candidate_id = 195802
	
UNION ALL
select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	--, c.experience_details_json
	, jsonb_array_elements(c.desired_industry_json::jsonb) as vc_candidate_ind
	, c.desired_industry_json
	--, c.desired_industry_json::json->0->>'desiredIndustryId'
	--, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as xyz
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_candidate_id = c.id
	where 1=1
	and c.desired_industry_json is not NULL
	--and m.vc_candidate_id = 42215
)

, merged_new as (select vc_candidate_id
	, array_to_json(array_agg(distinct pa_candidate_ind)) as new_candidate_ind
	from pa_candidate_ind
	where pa_candidate_ind is not NULL and pa_candidate_ind <> '{"desiredIndustryId": null}' and pa_candidate_ind <> '{"desiredIndustryId": ""}'
	group by vc_candidate_id)

--select * from merged_new --5610 rows
--where vc_candidate_id = 42215

--MAIN SCRIPT
update candidate c
set desired_industry_json = m.new_candidate_ind
from merged_new m
where m.vc_candidate_id = c.id
and m.vc_candidate_id = 42215


-->>MERGED FE/SFE<<--
with pa_candidate_fesfe as (select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as pa_candidate_fesfe
	, c.desired_functional_expertise_json
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and c.desired_functional_expertise_json is not NULL
	--and m.vc_candidate_id = 42215
	--and m.vc_pa_candidate_id = 195802
	
UNION ALL
select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as vc_candidate_fesfe
	, c.desired_functional_expertise_json
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_candidate_id = c.id
	where 1=1
	and c.desired_functional_expertise_json is not NULL
	--and m.vc_candidate_id = 42215
)

, merged_new as (select vc_candidate_id
	, array_to_json(array_agg(distinct pa_candidate_fesfe)) as new_candidate_fesfe
	from pa_candidate_fesfe
	where pa_candidate_fesfe is not NULL and pa_candidate_fesfe <> '{"desiredSubFunctionId": "", "desiredFunctionalExpertiseId": ""}'
	group by vc_candidate_id)

--select * from merged_new --9043 rows
--where vc_candidate_id = 42215

--MAIN SCRIPT
update candidate c
set desired_functional_expertise_json = m.new_candidate_fesfe
from merged_new m
where m.vc_candidate_id = c.id
--and m.vc_candidate_id = 42215

/* CHECK BEFORE INJECTION
select id, desired_functional_expertise_json
from candidate
where id = 42215
*/
