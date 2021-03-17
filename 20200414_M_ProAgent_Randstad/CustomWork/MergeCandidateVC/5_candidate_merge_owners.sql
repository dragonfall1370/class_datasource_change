with master_owner as (select m.candidate_id
	, m.master
	, jsonb_array_elements(c.candidate_owner_json::jsonb) as master_candidate_owner
	, c.candidate_owner_json
	, (jsonb_array_elements(c.candidate_owner_json::jsonb)->>'ownership')::int as ownership
	, (jsonb_array_elements(c.candidate_owner_json::jsonb)->>'ownerId')::int as ownerId
	, 1 as owner_index
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	left join candidate c on c.id = m.master
	where 1=1
	and c.candidate_owner_json is not NULL
	)
	
, slave_owner as (select m.candidate_id
	, m.master
	, jsonb_array_elements(c.candidate_owner_json::jsonb) as slave_candidate_owner
	, c.candidate_owner_json
	, (jsonb_array_elements(c.candidate_owner_json::jsonb)->>'ownership')::int as ownership
	, (jsonb_array_elements(c.candidate_owner_json::jsonb)->>'ownerId')::int as ownerId
	, 2 as owner_index
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	left join candidate c on c.id = m.candidate_id
	where 1=1
	and c.candidate_owner_json is not NULL
	--and m.master = 42215
	--and m.candidate_id = 195515
	)
	
, changed_owner as (
	select a.candidate_id
		, a.master
		, a.master_candidate_owner
		, b.slave_candidate_owner
		, b.ownerId
	from master_owner a
	inner join slave_owner b on a.master = b.master
	where a.ownership = 100 and b.ownership > 0
	and a.ownerId <> b.ownerId) --select * from changed_owner


, slave_candidate_owner as (select m.candidate_id
	, m.master
	, case when ma.master is not NULL then jsonb_set(candidate_owner_json::jsonb, '{ownership}', '0'::jsonb, false)
			else candidate_owner_json end as slave_candidate_owner
	--, c.candidate_owner_json
	, 2 as owner_index
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join (select id, jsonb_array_elements(candidate_owner_json::jsonb) as candidate_owner_json, (jsonb_array_elements(candidate_owner_json::jsonb)->>'ownerId')::int as ownerId from candidate) c on c.id = m.candidate_id
	left join changed_owner ma on ma.master = m.master and ma.ownerId = c.ownerId
	where 1=1
	and c.candidate_owner_json is not NULL

/* AUDIT
select *
from slave_candidate_owner
where master in (240855, 289123)

*/
UNION ALL
select m.candidate_id
	, m.master
	, jsonb_array_elements(c.candidate_owner_json::jsonb) as slave_candidate_owner
	--, c.candidate_owner_json
	, 1 as owner_index
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	left join candidate c on c.id = m.master
	where 1=1
	and c.candidate_owner_json <> ''
	--and m.master = 42215
)

, candidate_owner_distinct as (select distinct master
	, slave_candidate_owner
	from slave_candidate_owner) --select * from candidate_owner_distinct

, merged_new as (select master as master_candidate_id
	, array_to_json(array_agg(slave_candidate_owner)) as new_candidate_owner
	from candidate_owner_distinct
	where slave_candidate_owner is not NULL
	group by master) --select * from merged_new where master_candidate_id in (240855, 289123)

	
--MAIN SCRIPT
update candidate c
set candidate_owner_json = m.new_candidate_owner
from merged_new m
where m.master_candidate_id = c.id