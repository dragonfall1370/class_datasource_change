/* RUN THIS SCRIPT TO UPDATE CONTRACT/TEMP APPLICATION */

--UPDATE CONTRACT/TEMP PLACEMENT
WITH comp_contract as (
select  opi.id as offer_personal_id,
opi.start_date + interval '1 year' as end_date
from offer o
inner join offer_personal_info opi on opi.offer_id = o.id
where o.position_type <> 1
and opi.start_date is not null
--and opi.end_date is null
and (o.contract_rate_type is null)
and (o.contract_length_type is null)
and (o.contract_length is null or o.contract_length = 0)
)
update offer_personal_info
set end_date = cc.end_date
from comp_contract cc
where cc.offer_personal_id = offer_personal_info.id

--update contract_length_type, contract_rate_type
WITH subquery as (
select  o.id as offer_id
from offer o
where o.position_type <> 1
and (o.contract_rate_type is null)
and (o.contract_length_type is null)
and (o.contract_length is null or o.contract_length = 0)
)
UPDATE offer
SET contract_rate_type = 1
,contract_length_type = 4
,contract_length = 0
FROM subquery
WHERE offer.id = subquery.offer_id;


/* VERSION TO CHECK REFERENCE 

WITH comp_contract as (
	select  opi.id as offer_personal_id, o.contract_length, o.contract_length_type, o.contract_rate_type
	, opi.start_date, opi.end_date
	, case when opi.end_date is NULL then opi.start_date + interval '1 year' 
	else opi.end_date end as new_end_date
	from offer o
	inner join position_candidate pc on pc.id = o.position_candidate_id
	inner join offer_personal_info opi on opi.offer_id = o.id
	where pc.status >= 200 and pc.rejected_date is null
	and o.position_type <> 1 and (o.contract_length is null or o.contract_length = 0) and opi.start_date is not null 
	--and opi.end_date is null --
	--and opi.id = 1121
)
--select * from comp_contract
update offer_personal_info
set end_date = cc.new_end_date
, contract_length_type = 4 --Type: Month(s)
, contract_rate_type = 1 --Hourly
from offer_personal_info as a
inner join comp_contract cc on cc.offer_personal_id = a.id
where offer_personal_info.id = a.id
	and offer_personal_info.end_date is null

*/