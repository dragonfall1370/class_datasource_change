--Remove redundant Industry
with cand_filter as (select distinct candidate_id
	from candidate_work_history
	where industry < 29018
	)
	
, t1 as (
	SELECT id, json_array_elements(experience_details_json::json) AS ele FROM candidate
	where 1=1
	--and id = 292006
	and id in (select candidate_id from cand_filter)
) --select distinct id from t1

, t2 as (
	select t1.id, array_to_json(array_agg(case when (nullif(ele->>'industry', ''))::int < 29018 --some industries already ''
		then jsonb_set(t1.ele::jsonb, '{industry}'::text[],  '""'::jsonb, false)::json 
		else ele end)
	) AS json_value
	from t1
	--where ele->>'industry' <> '29028'
	group by t1.id
) --select * from t2

/*-- select jsonb_set(t1.ele::jsonb, '{industry}'::text[], '""'::jsonb, false) from t1 where (ele->>'industry')::int = 29028
select * from t2
limit 100
*/

--MAIN SCRIPT
update candidate
set experience_details_json = t2.json_value
from t2
where t2.id = candidate.id


---AUDIT CHECK
select id, experience_details_json
from candidate
where id = 292099



----Remove redundant SFE
with cand_filter as (select candidate_id, functional_expertise_id, sub_function_id
	from candidate_work_history
	where sub_function_id not in (select id from sub_functional_expertise)
	)

, t1 as (
	SELECT id, json_array_elements(experience_details_json::json) AS ele FROM candidate
	where 1=1
	--and id = 292006
	and id in (select candidate_id from cand_filter)
) --select * from t1 --425

, t2 as (
	select t1.id, array_to_json(array_agg(case when (nullif(ele->>'subFunctionId', ''))::int not in (select id from sub_functional_expertise) --some industries already ''
		then jsonb_set(t1.ele::jsonb, '{subFunctionId}'::text[],  '""'::jsonb, false)::json 
		else ele end)
	) AS json_value
	from t1
	group by t1.id
) --select * from t2 where id = 292131


--MAIN SCRIPT
update candidate
set experience_details_json = t2.json_value
from t2
where t2.id = candidate.id



---Remove redundant FE
