--CONTACT FE/SFE
with fe_sfe as (
	select fe.id as FEID
	, case when position('【PP】' in fe.name) > 0 then overlay(fe.name placing '' from 1 for length('【PP】')) 
		else fe.name end as FE
	, sfe.id as SFEID
	, sfe.name as SFE
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	)
	
select contact_id
, c.first_name
, c.last_name
, c.email
, fe.id as feid
, case when position('【PP】' in fe.name) > 0 then overlay(fe.name placing '' from 1 for length('【PP】')) 
		else fe.name end as FE
, sfe.id as sfeid
, sfe.name as SFE
from contact_functional_expertise cfe
left join contact c on c.id = cfe.contact_id
left join functional_expertise fe on fe.id = cfe.functional_expertise_id
left join sub_functional_expertise sfe on sfe.id = cfe.sub_functional_expertise_id
where cfe.functional_expertise_id < 3043 


---JOB FE/SFE
select pd.id
, pd.name
, fe.id as feid
, case when position('【PP】' in fe.name) > 0 then overlay(fe.name placing '' from 1 for length('【PP】')) 
		else fe.name end as FE
, sfe.id as sfeid
, sfe.name as SFE
from position_description_functional_expertise pfe
left join position_description pd on pd.id = pfe.position_id
left join functional_expertise fe on fe.id = pfe.functional_expertise_id
left join sub_functional_expertise sfe on sfe.id = pfe.sub_functional_expertise_id
where pfe.functional_expertise_id < 3043 
order by pd.id


--CANDIDATE DESIRED FE/SFE
with desired_fe as (select c.id
	, c.first_name
	, c.last_name
	, c.email
	, c.external_id
	, jsonb_array_elements(c.desired_functional_expertise_json::jsonb)->>'desiredFunctionalExpertiseId' as desired_fe_id
	from candidate c)
	
select d.id
	, d.first_name
	, d.last_name
	, d.email
	, d.external_id
	, d.desired_fe_id
	, case when position('【PP】' in fe.name) > 0 then overlay(fe.name placing '' from 1 for length('【PP】')) 
			else fe.name end as FE
from desired_fe d
join functional_expertise fe on fe.id = d.desired_fe_id::int --some old FEs are deleted already 
where 1=1
and nullif(d.desired_fe_id, '') is not NULL
and d.desired_fe_id::int < 3043
order by d.id


--CANDIDATE WORK HISTORY FE/SFE
select c.id, c.candidate_id
, can.first_name
, can.last_name
, can.email
, c.job_title
, c.current_employer
, c.functional_expertise_id
, case when position('【PP】' in fe.name) > 0 then overlay(fe.name placing '' from 1 for length('【PP】')) 
			else fe.name end as FE
, c.index
from candidate_work_history c
join functional_expertise fe on fe.id = c.functional_expertise_id::int
left join candidate can on can.id = c.candidate_id
where c.functional_expertise_id is not NULL
and c.functional_expertise_id < 3043


