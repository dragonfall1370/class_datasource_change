-->> MERGED INDUSTRY <<--
with pa_candidate_ind as (select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	--, c.experience_details_json
	, jsonb_array_elements(c.desired_industry_json::jsonb) as pa_candidate_ind
	, c.desired_industry_json
	--, c.desired_industry_json::json->0->>'desiredIndustryId'
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and c.desired_industry_json is not NULL
	
UNION ALL
select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	--, c.experience_details_json
	, jsonb_array_elements(c.desired_industry_json::jsonb) as vc_candidate_ind
	, c.desired_industry_json
	--, c.desired_industry_json::json->0->>'desiredIndustryId'
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_candidate_id = c.id
	where 1=1
	and c.desired_industry_json is not NULL
	--and m.vc_candidate_id = 42215
)

, t1 as (select *
	from pa_candidate_ind
	where pa_candidate_ind is not NULL and pa_candidate_ind <> '{"desiredIndustryId": null}' and pa_candidate_ind <> '{"desiredIndustryId": ""}'
	) --select * from t1
	
, t2 as (
	select vc_candidate_id
	, pa_candidate_ind->>'desiredIndustryId' as desiredIndustryId
	, pa_candidate_ind->>'desiredSubIndustryId' as desiredSubIndustryId
	from t1
	) --select * from t2

, t3 as (
	select distinct vc_candidate_id
	, desiredIndustryId
	from t2
	where desiredSubIndustryId is not null
	) --select * from t3

, merged_new as (SELECT vc_candidate_id, array_to_json(array_agg(pa_candidate_ind)) AS new_candidate_ind
	FROM t1
	where pa_candidate_ind->>'desiredSubIndustryId' is not null
	or not exists (select 1 from t3 where t3.vc_candidate_id = t1.vc_candidate_id and t3.desiredIndustryId = t1.pa_candidate_ind->>'desiredIndustryId')
	GROUP BY vc_candidate_id
	) --select * from merged_new
	
--MAIN SCRIPT
update candidate c
set desired_industry_json = m.new_candidate_ind
from merged_new m
where m.vc_candidate_id = c.id


-->> MERGED FE/SFE <<--
with pa_candidate_fesfe as (
	select *
	from (select m.vc_pa_candidate_id
		, m.vc_candidate_id
		, m.cand_ext_id
		, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as pa_candidate_fesfe
		, c.desired_functional_expertise_json
		from mike_tmp_candidate_dup_check m 
		join candidate c on m.vc_pa_candidate_id = c.id
		where 1=1
		and c.desired_functional_expertise_json is not NULL
		--and m.vc_candidate_id = 40932
		--and m.vc_pa_candidate_id = 195802
		) b
		where pa_candidate_fesfe <> '{"desiredSubFunctionId": "", "desiredFunctionalExpertiseId": ""}'
	
UNION ALL
select *
	from (
	select m.vc_pa_candidate_id
		, m.vc_candidate_id
		, m.cand_ext_id
		, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as vc_candidate_fesfe
		, c.desired_functional_expertise_json
		from mike_tmp_candidate_dup_check m 
		join candidate c on m.vc_candidate_id = c.id
		where 1=1
		and c.desired_functional_expertise_json is not NULL
		--and m.vc_candidate_id = 40932
		) a
	where vc_candidate_fesfe <> '{"desiredSubFunctionId": "", "desiredFunctionalExpertiseId": ""}'
)

, t1 as (select *
	from pa_candidate_fesfe
	where pa_candidate_fesfe is not NULL and pa_candidate_fesfe <> '{"desiredFunctionalExpertiseId": ""}' and pa_candidate_fesfe <> '{"desiredSubFunctionId": ""}'
	) --select * from t1
	
, t2 as (
	select vc_candidate_id
	, pa_candidate_fesfe->>'desiredFunctionalExpertiseId' as desiredFunctionalExpertiseId
	, pa_candidate_fesfe->>'desiredSubFunctionId' as desiredSubFunctionId
	from t1
	) --select * from t2

, t3 as (
	select distinct vc_candidate_id
	, desiredFunctionalExpertiseId
	from t2
	where desiredSubFunctionId is not null
	) --select * from t3

, merged_new as (SELECT vc_candidate_id, array_to_json(array_agg(pa_candidate_fesfe)) AS new_candidate_fesfe
	FROM t1
	where pa_candidate_fesfe->>'desiredSubFunctionId' is not null
	or not exists (select 1 from t3 where t3.vc_candidate_id = t1.vc_candidate_id and t3.desiredFunctionalExpertiseId = t1.pa_candidate_fesfe->>'desiredFunctionalExpertiseId')
	GROUP BY vc_candidate_id
	) --select * from merged_new
	
--MAIN SCRIPT
update candidate c
set desired_functional_expertise_json = m.new_candidate_fesfe
from merged_new m
where m.vc_candidate_id = c.id