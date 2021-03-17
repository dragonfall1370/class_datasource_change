with candidate_workhistory as
(--Current master work history
select m.candidate_id
	, m.master
	, jsonb_array_elements(c.experience_details_json::jsonb) as master_candidate_workhistory
	, c.experience_details_json
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	join candidate c on m.master = c.id
	where 1=1
	and c.experience_details_json is not NULL and trim(c.experience_details_json) <> '[]'
	and trim(c.experience_details_json) <> ''
	--and substring(c.experience_details_json, length(c.experience_details_json)) = ']' --for audit check
	--and m.vc_candidate_id = 41141
	--and m.vc_pa_candidate_id = 136920
-- rows

UNION ALL
--Slave work history
select m.candidate_id
	, m.master
	, jsonb_array_elements(c.experience_details_json::jsonb) as slave_candidate_workhistory
	, c.experience_details_json
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	join candidate c on m.candidate_id = c.id
	where 1=1
	and c.experience_details_json is not NULL
	--and m.vc_candidate_id = 41141
	--and m.vc_pa_candidate_id = 195515	
) -- rows

, t1 as (select distinct *
	from candidate_workhistory
	where candidate_id is not NULL and master_candidate_workhistory is not NULL
	) --select * from t1
	
, t2 as (
	select master
	, master_candidate_workhistory->>'industry' as IndustryId
	, master_candidate_workhistory->>'subIndustry' as SubIndustryId
	from t1
	)
	
, t3 as (
	select distinct master
	, IndustryId
	from t2
	where SubIndustryId is not null
	) --select * from t3
	
, merged_new as (SELECT master as master_candidate_id, array_to_json(array_agg(distinct master_candidate_workhistory)) AS new_candidate_workhistory
	FROM t1
	where 1=1
	--and vc_candidate_id = 41038
	and (master_candidate_workhistory->>'subIndustry' is not null
		or not exists (select 1 from t3 where t3.master = t1.master and t3.IndustryId = t1.master_candidate_workhistory->>'industry'))
	GROUP BY master
	) --select * from merged_new where master_candidate_id = 253189
	
/* AUDIT
select *
from mike_tmp_candidate_dup_name_mail_dob_master_slave
where master = 253189

select id, experience_details_json
from candidate
where id in (253189, 283340) --special case: (47494, 47495, 47493)
*/

--MAIN SCRIPT
update candidate c
set experience_details_json = m.new_candidate_workhistory
--from merged_new m --running directly
from merged_new m --running from tmp table
where m.master_candidate_id = c.id