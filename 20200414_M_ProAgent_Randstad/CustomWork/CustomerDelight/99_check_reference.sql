--Check reference
---20 selected candidate sources
select *
from candidate_source

--Reg date in 2.5 - 1.5 months ago
select id, insert_timestamp
, current_timestamp - interval '2.5 months' as date_from
, current_timestamp - interval '1.5 months' as date_to
from candidate
where 1=1
and deleted_timestamp is NULL
and insert_timestamp between current_timestamp - interval '2.5 months'  and current_timestamp - interval '1.5 months'


---1st interview date > Last stage date within 1 month
select id, candidate_id, position_description_id
, associated_date
, insert_timestamp
, status
, coalesce(interview1_date, associated_date) --used for data import
, last_stage_date
from position_candidate
where 1=1
and status > 102 --SENT stage
and last_stage_date between coalesce(interview1_date, associated_date) and coalesce(interview1_date, associated_date) + interval '1 month'

-->>CONCERN: If coming from data migration, interview1_date and last stage date may be the same


---Placement detail
select offer_id, placed_date, start_date
, now() - interval '6 months' as place_within
, now() - interval '1 month' as start_within
from offer_personal_info
where 1=1
and placed_date >= now() - interval '6 months'
and start_date >= now() - interval '1 month'
--and start_date >= now() - interval '3 months' --considered if no of records are low


--Start date and placed date within time frame
select offer_id, placed_date, start_date
	, current_timestamp - interval '6 months' as place_within
	, current_timestamp - interval '1 month' as start_within
	from offer_personal_info
	where 1=1
	and placed_date between now() - interval '6 months' and now()
	and start_date between now() - interval '1 month' and now()
	--and start_date >= now() - interval '3 months' --considered if no of records are low


--Start date greater than now()
select offer_id, placed_date, start_date
, current_timestamp - interval '6 months' as place_within
, current_timestamp - interval '1 month' as start_within
from offer_personal_info
where 1=1
and start_date >= now()
and placed_date >= now()
and placed_date >= now() - interval '6 months'
and start_date >= now() - interval '1 month'


--Contact having job with stages after 1st and last stage date within 1 month
select pc.id, pc.candidate_id, pc.position_description_id
, pc.associated_date
, pc.insert_timestamp
, pc.status
, coalesce(pc.interview1_date, pc.associated_date) --used for data import
, pc.last_stage_date
, pd.contact_id
from position_candidate pc
join position_description pd on pd.id = pc.position_description_id
where 1=1
and pc.status > 102 --SENT stage
and pc.last_stage_date between coalesce(pc.interview1_date, pc.associated_date) and coalesce(pc.interview1_date, pc.associated_date) + interval '1 month'