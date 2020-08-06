with jobapp as (select *
		from offer
		where position_candidate_id in (select id from mike_jobapp_tobemerged where status >= 200)
		)

--Offer having offer_personal_info
, offer_filter as (select ja.id as offer_id
		, ja.position_candidate_id
		, opi.*
		from jobapp ja
		left join offer_personal_info opi on ja.id = opi.offer_id
		where opi.offer_id is not NULL)

/* AUDIT COMPARISON CHECK
select opi.id
, o.id as offer_id
, opi.last_name
, opi.first_name
, opi.phone
, opi.home_phone
, opi.email
, opi.candidate_company_name
, opi.preferred_name
, pc.candidate_id
, c.external_id as cand_ext_id
, c.last_name
, c.first_name
, c.phone
, c.home_phone
, c.email
, c.company_name --aka. opi.candidate_company_name
, nullif(c.nickname, '') as nickname --aka. opi.preferred_name
, pd.external_id as job_ext_id
, pc.status
from offer_personal_info opi
left join offer o on o.id = opi.offer_id --get position_candidate_id
left join position_candidate pc on pc.id = o.position_candidate_id
left join candidate c on pc.candidate_id = c.id
left join position_description pd on pc.position_description_id = pd.id
where pc.id in (select position_candidate_id from offer_filter)
*/

--MAIN SCRIPT
update offer_personal_info opi
set last_name = c.last_name
, first_name = c.first_name
, phone = c.phone
, home_phone = c.home_phone
, email = c.email
, candidate_company_name = c.company_name
, preferred_name = nullif(c.nickname, '')
from offer o
		left join position_candidate pc on pc.id = o.position_candidate_id
		left join candidate c on pc.candidate_id = c.id
		left join position_description pd on pc.position_description_id = pd.id
where 1=1
and o.id = opi.offer_id --get position_candidate_id
and pc.id in (select position_candidate_id from offer_filter)


/* Reference check
select company_name
from candidate
limit 10

select *
from offer_personal_info --offer_id = 15327
order by id desc
limit 100


select *
from offer
where id = 15327
*/