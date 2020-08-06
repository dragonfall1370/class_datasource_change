--Contact FE/SFE to be deleted (SFE removed)
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

select cfe.contact_id
, c.first_name
, c.last_name
, c.phone
, c.email
, cfe.functional_expertise_id
, overlay(v.fe placing '' from 1 for length('【PP】')) as fe
, cfe.sub_functional_expertise_id
, v.sfe
from contact_functional_expertise cfe
left join contact c on c.id = cfe.contact_id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
where cfe.functional_expertise_id in (3046, 3051, 3055)
and cfe.sub_functional_expertise_id in (698, 756, 757, 758, 764, 822, 823, 824, 825) --7


--Job FE/SFE to be deleted (SFE removed)
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

select distinct pfe.position_id
, pd.name
, pfe.functional_expertise_id
, overlay(v.fe placing '' from 1 for length('【PP】')) as fe
, pfe.sub_functional_expertise_id
, v.sfe
from position_description_functional_expertise pfe
join position_description pd on pd.id = pfe.position_id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', pfe.functional_expertise_id, pfe.sub_functional_expertise_id)
where pfe.functional_expertise_id in (3046, 3051, 3055)
and pfe.sub_functional_expertise_id in (698, 756, 757, 758, 764, 822, 823, 824, 825)
order by pfe.position_id --341 > 328 unique job IDs


--Candidate desired FE/SFE to be deleted (SFE removed)
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

, desired_split as (select id, desired_functional_expertise_json
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredSubFunctionId' as SFEID
		, json_array_elements(desired_functional_expertise_json::json)->>'desiredFunctionalExpertiseId' as FEID
	from candidate
	where 1=1
	and deleted_timestamp is NULL
	and desired_functional_expertise_json is not NULL
	)
	
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
from desired_split d
join candidate c on c.id = d.id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', d.FEID, d.SFEID)
where nullif(d.feid, '')::int in (3046, 3051, 3055)
and nullif(d.sfeid, '')::int in (698, 756, 757, 758, 764, 822, 823, 824, 825)
order by d.id --193 > 182 unique candidate


--Candidate work history FE/SFE to be deleted (SFE removed)
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

select --distinct c.candidate_id
c.id, c.candidate_id
, can.first_name
, can.last_name
, can.email
, c.job_title
, c.current_employer
, c.functional_expertise_id
, c.sub_function_Id
, c.index
from candidate_work_history c
join candidate can on can.id = c.candidate_id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', c.functional_expertise_id, c.sub_function_id)
where c.functional_expertise_id in (3046, 3051, 3055)
and c.sub_function_id in (698, 756, 757, 758, 764, 822, 823, 824, 825)
order by c.candidate_id --1344 > 977 unique candidates