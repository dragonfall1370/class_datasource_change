with candidate_ind as (select distinct m.vc_new_ind_id --industry ID
		, ci.candidate_id
		from candidate_industry ci
		join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = ci.vertical_id
		
		UNION ALL
		select distinct m.vc_sub_ind_id --sub industry ID
		, ci.candidate_id
		from candidate_industry ci
		join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = ci.vertical_id --old company industry
)

/* --BACKUP
select * 
into mike_tmp_candidate_industry_20200423
from candidate_industry
*/

insert into candidate_industry (vertical_id, candidate_id, insert_timestamp)
select distinct vc_new_ind_id vertical_id
, candidate_id
, current_timestamp insert_timestamp
from candidate_ind
on conflict on constraint candidate_industry__pkey
	do nothing