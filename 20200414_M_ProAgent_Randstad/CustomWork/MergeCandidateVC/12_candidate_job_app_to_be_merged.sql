--JOB APP TO BE MERGED
with appfilter as (select pc.id, pc.candidate_id, m.master, pc.position_description_id, pc.status
	from position_candidate pc
	join mike_tmp_candidate_dup_name_mail_dob_master_slave m on m.candidate_id = pc.candidate_id --job app links with slave candidates
	where 1=1
	--and exists (select 1 from mike_tmp_candidate_dup_check where m.vc_pa_candidate_id = pc.candidate_id)
	--and pc.status < 200 --offered stage
	)

--JOB APP TO MERGED
select *
--into mike_tmp_req_jobapp_tobemerged_20200908
from appfilter --113


--MAIN SCRIPT TO UPDATE
update position_candidate pc
set candidate_id = af.master
from mike_tmp_req_jobapp_tobemerged_20200908 af
where pc.id = af.id
--and pc.candidate_id in (156924)


--->> JOB APP INFO HIGHER THAN OFFERED<<<---
with jobapp as (select *
		from offer
		where position_candidate_id in (select id from mike_tmp_req_jobapp_tobemerged_20200908 where status >= 200)
		)

--Offer having offer_personal_info
, offer_filter as (select ja.position_candidate_id
		, opi.*
		from jobapp ja
		left join offer_personal_info opi on ja.id = opi.offer_id
		where opi.offer_id is not NULL) --select * from offer_filter


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
set phone = c.phone
, last_name = c.last_name --no changes
, first_name = c.first_name --no changes
, home_phone = c.home_phone
--, email = c.email --no changes
--, candidate_company_name = c.company_name --no changes
, preferred_name = nullif(c.nickname, '')
from offer o
		left join position_candidate pc on pc.id = o.position_candidate_id
		left join candidate c on pc.candidate_id = c.id
		left join position_description pd on pc.position_description_id = pd.id
where 1=1
and o.id = opi.offer_id --get position_candidate_id
and pc.id in (select position_candidate_id from offer_filter)