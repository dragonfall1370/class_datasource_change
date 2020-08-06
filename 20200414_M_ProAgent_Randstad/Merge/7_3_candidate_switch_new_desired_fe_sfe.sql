--MIGRATE OLD FE/SFE TO NEW FE/SFE
with desired_split as (select id, desired_functional_expertise_json
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredSubFunctionId' as SFEID
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredFunctionalExpertiseId' as FEID
	from candidate
	where 1=1
	and (external_id is NULL or external_id not ilike 'CDT%')
	and deleted_timestamp is NULL
	and desired_functional_expertise_json is not NULL
	)

/* BACKUP CURRENT DESIRED INDUSTRIES
select id, first_name, last_name, email, external_id, insert_timestamp, desired_functional_expertise_json
--into mike_tmp_candidate_desired_functional_expertise_json_20200705
from candidate
where desired_functional_expertise_json is not NULL --76070
*/

, merged_new as (select c.id, c.FEID, c.SFEID
	, m.vcfeid
	, m.vcsfeid
	, m.vc_new_fe_en
	, m.vc_new_sfe_split
	from desired_split c
	join mike_tmp_vc_2_vc_new_fe_sfe m on m.vc_fe_id = c.FEID::int and m.vc_sfe_id = c.SFEID::int
	where c.FEID <> '' and c.SFEID <> ''
	) --select * from merged_new --142

, desired_fe_sfe as (SELECT id
	, json_agg(row_to_json((
				SELECT ColumnName 
				FROM (SELECT vcsfeid::text, vcfeid::text) AS ColumnName ("desiredSubFunctionId", "desiredFunctionalExpertiseId")
				)) order by vcfeid, vcsfeid) AS json_value
	FROM merged_new
	GROUP BY id)

update candidate c
set desired_functional_expertise_json = json_value
from desired_fe_sfe d
where c.id = d.id