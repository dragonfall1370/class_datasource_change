--UPDATE OFFER PERSONAL INFO > FIRST NAME AFTER MERGED
---Conditions
with jobapp as (select pc.id as jobappid
	, pc.position_description_id
	, pc.candidate_id
	from position_candidate pc
	left join position_description pd on pd.id = pc.position_description_id
	left join candidate c on c.id = pc.candidate_id
	where 1=1
	and pd.external_id ilike 'JOB%'
	and c.external_id ilike 'CDT%'
	and pc.status >= 200
) --12423

, migrated_offer as (select *
	from offer_personal_info
	where 1=1
	--and insert_timestamp between '2020-07-05 08:30:00' and '2020-07-05 08:50:00'
	and offer_id in (select id from offer where position_candidate_id in (select jobappid from jobapp))
	--order by insert_timestamp desc
	)

/* AUDIT CHANGED RESULT

select id, email
, overlay(email placing '' from 1 for 10 ) as email
, replace(first_name, '氏名', N'　') as first_name
from migrated_offer
where 1=1
and overlay(email placing '' from 1 for 10) <> 'candidate@noemail.com'

*/

---UDPATE FIRST NAME
update offer_personal_info
set first_name = N'　'
where 1=1
and first_name = '氏名'
and offer_id in (select offer_id from migrated_offer)


---UDPATE EMAIL
update offer_personal_info
set email = overlay(email placing '' from 1 for 10 )
where 1=1
and offer_id in (select offer_id from migrated_offer)
and overlay(email placing '' from 1 for 10 ) <> 'candidate@noemail.com'