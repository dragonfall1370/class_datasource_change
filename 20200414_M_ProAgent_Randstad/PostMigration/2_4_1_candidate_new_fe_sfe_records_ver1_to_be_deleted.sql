/* >> CANDIDATE DESIRED FE/SFE << */
--Candidate FE/SFE in conversion 1 to be changed
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

--Candidates list with original FE/SFE to be converted
, desired_split as (select id, desired_functional_expertise_json
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredSubFunctionId' as SFEID
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredFunctionalExpertiseId' as FEID
	from mike_tmp_candidate_desired_functional_expertise_json_20200705
	where 1=1
	and desired_functional_expertise_json is not NULL
	)

, cand_desired_fe_sfe_bkup as (select distinct d.id
	from desired_split d
	left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', d.FEID, d.SFEID)
	where 1=1
	and nullif(d.FEID, '')::int in (3004, 3001)
	and nullif(d.SFEID, '')::int in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
	)

--Candidates list with current FE/SFE
, cand_desired_split as (select id, desired_functional_expertise_json
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredSubFunctionId' as SFEID
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredFunctionalExpertiseId' as FEID
	from candidate
	where 1=1
	and deleted_timestamp is NULL
	and desired_functional_expertise_json is not NULL
	) --select * from cand_desired_split --354637

--Candidate list with current conversion
select distinct d.id
	, c.first_name
	, c.last_name
	, c.phone
	, c.email
	, d.FEID
	, overlay(v.fe placing '' from 1 for length('【PP】')) as fe
	, d.SFEID
	, v.sfe
--distinct id
from cand_desired_split d
join candidate c on c.id = d.id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', d.FEID, d.SFEID)
where 1=1
and nullif(d.FEID, '')::int in (3044, 3046, 3047, 3051, 3052, 3055, 3058, 3059) --8 FEs in conversion 1
and nullif(d.SFEID, '')::int in (657, 685, 686, 695, 755, 756, 757, 758, 759, 760, 763, 764, 766, 790, 799, 805, 807, 822, 823, 824, 825, 827, 855, 858, 861, 866, 869, 872, 906)
and d.id in (select id from cand_desired_fe_sfe_bkup) -- rows
order by d.id --1035 unique candidate IDs















/* >> CANDIDATE WORK HISTORY FE/SFE << */
--Candidate work history FE/SFE to be deleted (SFE removed)
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

--Backup candidate work history
, wh_split as (select id, experience_details_json
		, jsonb_array_elements(experience_details_json::jsonb)->>'subFunctionId' as SFEID
		, jsonb_array_elements(experience_details_json::jsonb)->>'functionalExpertiseId' as FEID
	from mike_tmp_experience_details_json_20200705 --using backup experience instead
	where 1=1
	and experience_details_json is not NULL and trim(experience_details_json) <> '[]'
	and trim(experience_details_json) <> '' and trim(experience_details_json) <> 'null'
	--and substring(experience_details_json, length(experience_details_json)) = ']'
	)

, cand_wh_fe_sfe_bkup as (select distinct d.id
	from wh_split d
	left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', d.FEID, d.SFEID)
	where 1=1
	and nullif(d.FEID, '')::int in (3004, 3001)
	and nullif(d.SFEID, '')::int in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
	) --select * from cand_wh_fe_sfe_bkup

/* --Current candidate work history | Using candidate_work_history instead
, cand_wh_split as (select id, experience_details_json
		, jsonb_array_elements(experience_details_json::jsonb)->>'subFunctionId' as SFEID
		, jsonb_array_elements(experience_details_json::jsonb)->>'functionalExpertiseId' as FEID
	from candidate
	where 1=1
	and experience_details_json is not NULL and trim(experience_details_json) <> '[]'
	and trim(experience_details_json) <> '' and trim(experience_details_json) <> 'null'
	--and substring(experience_details_json, length(experience_details_json)) = ']'
	)
*/
	
select --distinct c.candidate_id
c.id, c.candidate_id
, can.first_name
, can.last_name
, c.job_title
, c.current_employer
, c.functional_expertise_id
, overlay(v.fe placing '' from 1 for length('【PP】')) as fe
, c.sub_function_Id
, v.sfe
, c.index
from candidate_work_history c
join candidate can on can.id = c.candidate_id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', c.functional_expertise_id, c.sub_function_id)
where c.functional_expertise_id in (3044, 3046, 3047, 3051, 3052, 3055, 3058, 3059) --8 FEs in conversion 1
and c.sub_function_id in (657, 685, 686, 695, 755, 756, 757, 758, 759, 760, 763, 764, 766, 790, 799, 805, 807, 822, 823, 824, 825, 827, 855, 858, 861, 866, 869, 872, 906)
and c.candidate_id in (select id from cand_wh_fe_sfe_bkup)
order by c.candidate_id -- unique candidates