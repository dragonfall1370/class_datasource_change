with desired_split as (select id, external_id, desired_industry_json
		, json_array_elements(desired_industry_json::json)->>'desiredIndustryId' as industryID
		, json_array_elements(desired_industry_json::json)->>'desiredSubIndustryId' as subindustryID
	from candidate
	where 1=1
	and (external_id is NULL or external_id not ilike 'CDT%')
	and deleted_timestamp is NULL
	and desired_industry_json is not NULL
	--and id=40911
	) 

/* BACKUP CURRENT DESIRED INDUSTRIES
select id, first_name, last_name, email, external_id, insert_timestamp
--into mike_tmp_candidate_desired_industry_json_20200423
from candidate
where desired_industry_json is not NULL --76070
*/
	
, merged_new as (select c.id, c.industryID
	, m.vc_ind_id
	, m.vc_new_ind_id
	, concat_ws(' / ', m.vc_new_ind_en, m.vc_new_ind_ja) as industry
	, m.vc_sub_ind_id
	, m.vc_sub_ind_name
	from desired_split c
	join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = c.industryID::int
	where c.industryID <> ''
	) --select id from merged_new group by id --2439 rows
	
, desired_ind as (SELECT id
	, json_agg(row_to_json((
				SELECT ColumnName 
				FROM (SELECT vc_new_ind_id::text, vc_sub_ind_id::text) AS ColumnName ("desiredIndustryId", "desiredSubIndustryId")
					))  order by vc_ind_id, vc_sub_ind_id) AS json_value
	FROM merged_new
	GROUP BY id)
	
update candidate c
set desired_industry_json = json_value
from desired_ind d
where c.id = d.id