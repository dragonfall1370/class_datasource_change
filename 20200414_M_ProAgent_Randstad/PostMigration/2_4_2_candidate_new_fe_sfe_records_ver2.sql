--DRAFT CONVERSION 2 | DESIRED FE/SFE
with 
--Candidates list with original FE/SFE to be converted
 desired_split as (select id, desired_functional_expertise_json
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredSubFunctionId' as SFEID
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredFunctionalExpertiseId' as FEID
	from mike_tmp_candidate_desired_functional_expertise_json_20200705
	where 1=1
	and desired_functional_expertise_json is not NULL
	)

--Candidate list with current conversion
select distinct d.id
	, c.first_name
	, c.last_name
	, c.phone
	, c.email
	, d.FEID
	, m.vc_fe_name
	, d.SFEID
	, m.vc_sfe_name
	, m.vcfeid
	, overlay(m.vc_new_fe placing '' from 1 for length('【PP】')) as vc_new_fe
	, m.vcsfeid
	, m.vc_new_sfe_split
--distinct id
from desired_split d
join candidate c on c.id = d.id
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', d.FEID, d.SFEID)
where 1=1
and nullif(d.FEID, '')::int in (3004, 3001) --changed FE/SFE mapping from backup
and nullif(d.SFEID, '')::int in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
order by d.id -- unique candidate IDs


--WORK HISTORY FE/SFE
select --distinct c.candidate_id
c.id
, c.candidate_id
, can.first_name
, can.last_name
, c.job_title
, c.current_employer
, c.functional_expertise_id
, m.vc_fe_name
, c.sub_function_Id
, m.vc_sfe_name
, m.vcfeid
, overlay(m.vc_new_fe placing '' from 1 for length('【PP】')) as vc_new_fe
, m.vcsfeid
, m.vc_new_sfe_split
, c.index
from mike_tmp_candidate_work_history_20200705 c
join candidate can on can.id = c.candidate_id
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', c.functional_expertise_id, c.sub_function_Id)
where 1=1
and c.functional_expertise_id in (3004, 3001) --changed FE/SFE mapping from backup
and c.sub_function_Id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
order by c.id, c.candidate_id