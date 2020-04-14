--ADDITIONAL CONTACTS MERGED
--MERGED MOBILE
with merged_phone as (select m.merged_contact_id
	, c2.phone as merged_contact_phone
	, c2.mobile_phone as merged_mobile_phone
	, m.contact_id
	, c.mobile_phone
	, c.phone
	from mike_tmp_contact_dup_check2 m
	left join contact c on c.id = m.contact_id
	left join contact c2 on c2.id =  m.merged_contact_id
	where m.rn = 1
	and m.contact_id not in (select contact_id from mike_tmp_contact_dup_check))
	
, merged_phone_group as (select merged_contact_id
	, string_agg(mobile_phone, chr(10)) as mobile_phone_group
	, string_agg(phone, chr(10)) as phone_group
	from merged_phone
	where mobile_phone is not NULL or phone is not NULL
	group by merged_contact_id)

, merged_new as (select c.id
	, m.merged_contact_id
	, c.phone
	, m.phone_group
	, case when c.phone is NULL or c.phone = '' then m.phone_group
			else NULL end as new_phone
	, c.mobile_phone
	, m.mobile_phone_group
	, case when c.phone is NULL or c.phone = '' then concat_ws(',', c.mobile_phone, m.mobile_phone_group) --mobile phone only
			else concat_ws(',', c.mobile_phone, m.phone_group, m.mobile_phone_group) end as new_mobile_phone --phone & mobile phone
	from contact c
	join merged_phone_group m on m.merged_contact_id = c.id)

--UPDATE MOBILE
update contact c
set mobile_phone = m.new_mobile_phone
from merged_new m
where c.id = m.id
and m.new_mobile_phone is not NULL --430 rows

--UPDATE PHONE	
update contact c
set phone = m.new_phone
from merged_new m
where c.id = m.id
and c.phone is NULL or c.phone = '' --62 rows