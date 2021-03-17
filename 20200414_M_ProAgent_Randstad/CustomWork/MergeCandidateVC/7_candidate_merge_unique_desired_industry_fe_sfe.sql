-->> MERGED INDUSTRY <<--
with cand_desired_ind as (select m.candidate_id
	, m.master
	--, c.experience_details_json
	, jsonb_array_elements(c.desired_industry_json::jsonb) as cand_desired_ind
	, c.desired_industry_json
	--, c.desired_industry_json::json->0->>'desiredIndustryId'
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	join candidate c on m.master = c.id
	where 1=1
	and c.desired_industry_json is not NULL
	
UNION ALL
select m.candidate_id
	, m.master
	--, c.experience_details_json
	, jsonb_array_elements(c.desired_industry_json::jsonb) as cand_desired_ind
	, c.desired_industry_json
	--, c.desired_industry_json::json->0->>'desiredIndustryId'
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	join candidate c on m.candidate_id = c.id
	where 1=1
	and c.desired_industry_json is not NULL
	--and m.vc_candidate_id = 42215
)

, t1 as (select *
	from cand_desired_ind
	where cand_desired_ind is not NULL and cand_desired_ind <> '{"desiredIndustryId": null}' and cand_desired_ind <> '{"desiredIndustryId": ""}'
	) --select * from t1
	
, t2 as (
	select master
	, cand_desired_ind->>'desiredIndustryId' as desiredIndustryId
	, cand_desired_ind->>'desiredSubIndustryId' as desiredSubIndustryId
	from t1
	) --select * from t2

, t3 as (
	select distinct master
	, desiredIndustryId
	from t2
	where desiredSubIndustryId is not null
	) --select * from t3

, merged_new as (SELECT master as master_candidate_id
	, array_to_json(array_agg(distinct cand_desired_ind)) AS new_cand_desired_ind
	FROM t1
	where cand_desired_ind->>'desiredSubIndustryId' is not null
	or not exists (select 1 from t3 where t3.master = t1.master and t3.desiredIndustryId = t1.cand_desired_ind->>'desiredIndustryId')
	GROUP BY master
	) --select * from merged_new where master_candidate_id = 47493
	
/* AUDIT ORIGINAL DATA

select *
from mike_tmp_candidate_dup_name_mail_dob_master_slave
where master = 47493

select id, desired_industry_json
from candidate
where id in (47494, 47495, 47493)

*/

--MAIN SCRIPT
update candidate c
set desired_industry_json = m.new_cand_desired_ind
from merged_new m
where m.master_candidate_id = c.id



-->> MERGED FE/SFE <<--
with cand_desired_fesfe as (
	--MASTER
	select *
	from (select m.candidate_id
		, m.master
		, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as cand_desired_fesfe
		, c.desired_functional_expertise_json
		from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
		join candidate c on m.master = c.id
		where 1=1
		and c.desired_functional_expertise_json is not NULL
		--and m.vc_candidate_id = 40932
		--and m.vc_pa_candidate_id = 195802
		) b
		where cand_desired_fesfe <> '{"desiredSubFunctionId": "", "desiredFunctionalExpertiseId": ""}'
	
	UNION ALL
	
	--SLAVE
	select *
	from (select m.candidate_id
		, m.master
		, jsonb_array_elements(c.desired_functional_expertise_json::jsonb) as cand_desired_fesfe
		, c.desired_functional_expertise_json
		from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
		join candidate c on m.candidate_id = c.id
		where 1=1
		and c.desired_functional_expertise_json is not NULL
		--and m.vc_candidate_id = 40932
		) a
	where cand_desired_fesfe <> '{"desiredSubFunctionId": "", "desiredFunctionalExpertiseId": ""}'
)

, t1 as (select *
	from cand_desired_fesfe
	where cand_desired_fesfe is not NULL and cand_desired_fesfe <> '{"desiredFunctionalExpertiseId": ""}' and cand_desired_fesfe <> '{"desiredSubFunctionId": ""}'
	) --select * from t1
	
, t2 as (
	select master
	, cand_desired_fesfe->>'desiredFunctionalExpertiseId' as desiredFunctionalExpertiseId
	, cand_desired_fesfe->>'desiredSubFunctionId' as desiredSubFunctionId
	from t1
	) --select * from t2

, t3 as (
	select distinct master
	, desiredFunctionalExpertiseId
	from t2
	where desiredSubFunctionId is not null
	) --select * from t3

, merged_new as (SELECT master as master_candidate_id
	, array_to_json(array_agg(cand_desired_fesfe)) AS new_cand_desired_fesfe
	FROM t1
	where cand_desired_fesfe->>'desiredSubFunctionId' is not null
	or not exists (select 1 from t3 where t3.master = t1.master and t3.desiredFunctionalExpertiseId = t1.cand_desired_fesfe->>'desiredFunctionalExpertiseId')
	GROUP BY master
	) --select * from merged_new
	
--MAIN SCRIPT
update candidate c
set desired_functional_expertise_json = m.new_cand_desired_fesfe
from merged_new m
where m.master_candidate_id = c.id