with split as (select id, experience_details_json
, jsonb_array_elements(experience_details_json::jsonb) as ele
, jsonb_array_elements(experience_details_json::jsonb)->>'currentEmployer' as currentEmployer
, jsonb_array_elements(experience_details_json::jsonb)->>'functionalExpertiseId' as FEID
, jsonb_array_elements(experience_details_json::jsonb)->>'subFunctionId' as SFEID
from candidate
where experience_details_json is not NULL
and deleted_timestamp is NULL
and id = 109364
)

, merged_new as (select c.id, c.FEID, c.SFEID
	, m.vcfeid
	, m.vcsfeid
	, m.vc_new_fe_en
	, m.vc_new_sfe_split
	from split c
	join mike_tmp_vc_2_vc_new_fe_sfe m on m.vc_fe_id = c.FEID::int and m.vc_sfe_id = c.SFEID::int
	)
	
	select * from merged_new
	
--
with t1 as 
(
	SELECT id, experience_details_json
	, json_array_elements(experience_details_json::json) AS ele 
	from candidate
	where experience_details_json is not NULL
	and deleted_timestamp is NULL
	and id = 40910
	)  

, t2 as (
SELECT id 
, t1.ele->>'functionalExpertiseId' as vcfeid_old, t1.ele->>'subFunctionId' as vcsfeid_old, mike.vcfeid, mike.vcsfeid
, t1.ele
​
, jsonb_set(jsonb_set(ele::jsonb
, '{functionalExpertiseId}'
, ('"' || mike.vcfeid::varchar || '"')::jsonb
, false  
)
, '{subFunctionId}'
, ('"' || mike.vcsfeid::varchar || '"')::jsonb
, false  
) as ele_1
from t1 
left join mike_tmp_vc_2_vc_new_fe_sfe mike 
on t1.ele->>'functionalExpertiseId' = mike.vc_fe_id::varchar
and t1.ele->>'subFunctionId' = mike.vc_sfe_id::varchar
order by id, ele->>'dateRangeFrom'
)
, t3 as (
select id, array_to_json(array_agg(ele_1)) AS json_value
from t2
GROUP BY id
)