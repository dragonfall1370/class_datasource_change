--Company industry - using old tags
select ci.company_id
, c.name as company_name
, ci.industry_id
, v.name as industry
, c.external_id
from company_industry ci
left join vertical v on v.id = ci.industry_id
left join company c on c.id = ci.company_id
where ci.industry_id < 29018 --industry added before new master list
order by ci.company_id desc --84 rows


--Contact industry - using old tags
select ci.contact_id
, c.first_name
, c.last_name
, ci.industry_id
, v.name as industry
, c.external_id
from contact_industry ci
left join vertical v on v.id = ci.industry_id
left join contact c on c.id = ci.contact_id
where ci.industry_id < 29018 --industry added before new master list
order by ci.contact_id desc --56 rows


--Job industry - using old tags
select pi.position_id
, pd.name as job_title
, pi.industry_id
, v.name as industry
, pd.external_id
from position_description_industry pi
left join vertical v on v.id = pi.industry_id
left join position_description pd on pd.id = pi.position_id
where pi.industry_id < 29018
order by pi.position_id --97 rows


--Candidate desired industry - using old tags
with desired_ind as (select c.id
	, c.first_name
	, c.last_name
	, c.external_id
	, jsonb_array_elements(c.desired_industry_json::jsonb)->>'desiredIndustryId' as desired_industry_id
	from candidate c)
	
select d.id
	, d.first_name
	, d.last_name
	, d.external_id
	, d.desired_industry_id
	, v.name as industry
from desired_ind d
join vertical v on v.id = d.desired_industry_id::integer --some old industries are deleted already
where 1=1
and nullif(d.desired_industry_id,'') is not NULL
and d.desired_industry_id::integer < 29018
order by d.id


/* Check if valid json format
select id
, experience_details_json
, is_valid_json(experience_details_json)
, left(experience_details_json, 1)
from candidate
where 1=1
and is_valid_json(experience_details_json)
and left(trim(experience_details_json), 1) <> '['
--and nullif(experience_details_json, '') is not NULL
*/


--Candidate work history industry - using old tags
select c.id, c.candidate_id
, can.first_name
, can.last_name
, can.email
, c.job_title, c.current_employer
, c.industry industry_id
, v.name as industry
, c.index
from candidate_work_history c
left join vertical v on v.id = c.industry::int
left join candidate can on can.id = c.candidate_id
where c.industry is not NULL
and c.industry < 29018

