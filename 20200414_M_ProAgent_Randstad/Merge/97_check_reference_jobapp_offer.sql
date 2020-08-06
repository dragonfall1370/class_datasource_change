-->>Reference check for offer / offer_personal_info
with jobapp as (select *
		from offer
		where position_candidate_id in (select id from mike_jobapp_tobemerged where status >= 200)
		)

--Offer not having offer_personal_info
, offer_filter as (select ja.id as offer_id
		, ja.position_candidate_id
		from jobapp ja
		left join offer_personal_info opi on ja.id = opi.offer_id
		where opi.offer_id is NULL)

select pd.id
, c.external_id as cand_ext_id
, pd.external_id
from position_candidate pc
left join candidate c on pc.candidate_id = c.id
left join position_description pd on pc.position_description_id = pd.id
where pc.id in (select position_candidate_id from offer_filter)


-->>Reference check for existing offer / offer_personal_info
with jobapp as (select *
		from offer
		where position_candidate_id in (select id from mike_jobapp_tobemerged where status >= 200)
		)

--Offer having offer_personal_info
, offer_filter as (select ja.id as offer_id
		, ja.position_candidate_id
		from jobapp ja
		left join offer_personal_info opi on ja.id = opi.offer_id
		where opi.offer_id is not NULL)
		
select pd.id
, c.external_id as cand_ext_id
, pd.external_id
from position_candidate pc
left join candidate c on pc.candidate_id = c.id
left join position_description pd on pc.position_description_id = pd.id
where pc.id in (select position_candidate_id from offer_filter)