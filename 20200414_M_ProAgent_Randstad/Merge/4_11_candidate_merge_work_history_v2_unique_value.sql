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

, t1 as (select distinct *
	from pa_candidate_workhistory
	where vc_pa_candidate_id is not NULL and vc_workhistory is not NULL
	)
	
, t2 as (
	select vc_candidate_id
	, vc_workhistory->>'industry' as IndustryId
	, vc_workhistory->>'subIndustry' as SubIndustryId
	from t1
	)
	
, t3 as (
	select distinct vc_candidate_id
	, IndustryId
	from t2
	where SubIndustryId is not null
	) --select * from t3
	
, merged_new as (SELECT vc_candidate_id, array_to_json(array_agg(distinct vc_workhistory)) AS new_candidate_workhistory
	FROM t1
	where 1=1
	--and vc_candidate_id = 41038
	and (vc_workhistory->>'subIndustry' is not null
		or not exists (select 1 from t3 where t3.vc_candidate_id = t1.vc_candidate_id and t3.IndustryId = t1.vc_workhistory->>'industry'))
	GROUP BY vc_candidate_id
	)
	
/* VC TEMP TABLE
select *
into mike_merged_new_experience_details_json_20200706
from merged_new
*/
	
--MAIN SCRIPT
update candidate c
set experience_details_json = m.new_candidate_workhistory
--from merged_new m --running directly
from mike_merged_new_experience_details_json_20200706 m --running from tmp table
where m.vc_candidate_id = c.id