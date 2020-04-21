--Dual contact / candidate
select 'add_con_info' as additional_type
, id as contact_id
, 1007 as form_id
, 11272 as field_id
, dual_candidate_contact_c as field_value
, now() as insert_timestamp
from contact
where recordtypeid in ('0120Y0000013O5d')
and dual_candidate_contact_c = '1' --110 rows

/* CHECK SCRIPT

select ts2_legacy_contactid_c
, legacy_personid_c
, dual_candidate_contact_c
, *
from contact
where 1=1
and firstname = 'Andy'
and lastname = 'Bellis'
--and (ts2_legacy_contactid_c ilike '%49773%' or legacy_personid_c ilike '%49773%')
--and dual_candidate_contact_c = '1'

select ts2_legacy_contactid_c
, legacy_personid_c
, dual_candidate_contact_c
, *
from contact
where 1=1
and firstname = 'Tony'
and lastname = 'Reeve'
--and (ts2_legacy_contactid_c ilike '%49773%' or legacy_personid_c ilike '%49773%')
--and dual_candidate_contact_c = '1'


select ts2_legacy_contactid_c
, legacy_personid_c
, *
from contact
where 1=1
and firstname ilike '%Grzegorz%'
and lastname ilike '%Ostromecki%' --155441_CAND
--and (ts2_legacy_contactid_c ilike '%49773%' or legacy_personid_c ilike '%49773%')
and dual_candidate_contact_c = '1'

*/