with max_master_index as (select cf.candidate_id
		, max(index) as max_index
		from mike_tmp_candidate_dup_name_mail_dob_master_slave m
		left join configurable_form_group_value cf on cf.candidate_id = m.master
		where 1=1
		and parent_id = 11305
		group by cf.candidate_id)

, slave_cand as (select cf.id
	, cf.candidate_id --slave cand
	, cf.parent_id
	, cf.children_id
	, cf.text_data
	, cf.value_data
	, cf.value_data_array
	, cf.date_data
	, cf.index
	, cf.insert_timestamp
	, cf.constraint_id
	, m.master
	, coalesce(cf.index + mi.max_index + 1, 0) as new_index
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join configurable_form_group_value cf on cf.candidate_id = m.candidate_id
	left join max_master_index mi on mi.candidate_id = m.master --master index
	where 1=1
	and parent_id = 11305
	--and children_id = 11323 
	order by m.candidate_id, children_id) --select * from slave_cand where master = 47493

/* AUDIT ALL SLAVE AND MASTER CANDIDATE HISTORIES

, merged_new as (
	select master as master_candidate_id
	, parent_id
	, children_id
	, text_data
	, value_data
	, value_data_array
	, date_data
	, new_index
	, insert_timestamp
	, constraint_id
	from slave_cand

	UNION ALL
	select cf.candidate_id as master_candidate_id
	, parent_id
	, children_id
	, text_data
	, value_data
	, value_data_array
	, date_data
	, index
	, insert_timestamp
	, constraint_id
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join configurable_form_group_value cf on cf.candidate_id = m.master
	where 1=1
	and parent_id = 11305
	--and children_id = 11323
	--and cf.candidate_id = 47493
	--order by m.master, children_id
)

select * from merged_new
where master_candidate_id = 47493
order by master_candidate_id, new_index, children_id

*/


--MAIN SCRIPT
insert into configurable_form_group_value (candidate_id, parent_id, children_id, text_data, index)
select master as candidate_id
, parent_id
, children_id
, text_data
, value_data
, value_data_array
, date_data
, new_index as index
, insert_timestamp
, constraint_id
from slave_cand