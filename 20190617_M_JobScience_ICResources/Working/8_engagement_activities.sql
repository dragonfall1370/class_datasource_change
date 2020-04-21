---ENGAGEMENT ACTIVITIES
select a.id --known as engagement activities
, a.ts2_contact_c --contact | candidate
, con.id as "contact_id", con.fullname as "contact_name"
, can.id as "candidate_id", can.fullname as "candidate_name"
, a.ts2_recipient_address_c
, a.ts2_engagement_c
, a.ts2_engagement_stage_c
, s.name as engagement_stage_name
, concat_ws(chr(10), '[ENGAGEMENT ACTIVITIES]'
	, coalesce('Engagement activity name: ' || nullif(a.name,''),NULL)
	, coalesce('Currency: ' || nullif(a.currencyisocode,''),NULL)
	, coalesce('Engagement name: ' || nullif(s.name,'') || chr(10),NULL)
	, coalesce('Created By: ' || nullif(u.fullname,''),NULL)
	, coalesce('Created: ' || nullif(a.createddate,''),NULL)
	, coalesce('Last Modified By: ' || nullif(u2.fullname,''),NULL)
	, coalesce('Last Modified: ' || nullif(a.lastmodifieddate,''),NULL)
	, coalesce('Recipient address: ' || nullif(a.ts2_recipient_address_c,''),NULL)
	) as "content"
, a.createddate::timestamp as insert_timestamp
, -10 as user_account_id
, 'comment' as "category"
, 'contact' as "type"
from ts2_engagement_activity_c a
left join ts2_engagement_stage_c s on a.ts2_engagement_stage_c = s.ts2_engagement_stage_c_1_id
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u on u.id = a.createdbyid
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u2 on u2.id = a.lastmodifiedbyid
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact 
			where recordtypeid in ('0120Y0000013O5d')) con on con.id = a.ts2_contact_c --CONTACT
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact 
			where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')) can on can.id = a.ts2_contact_c--CANDIDATE
where 1=1
--and ts2_contact_c = '0030Y00000cqcXVQAY'
--and c.recordtypeid in ('0120Y0000013O5d') --Contact type
--and c.isdeleted = '0'
and a.ts2_contact_c is not NULL
and a.createddate::timestamp > '2017-10-17' --29295