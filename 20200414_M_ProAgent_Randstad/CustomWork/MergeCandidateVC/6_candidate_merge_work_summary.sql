with merged_experience as (select m.master
	, m.candidate_id
	, concat_ws('<br/>', ('【Merged from candidate: ' || m.candidate_id || '】') , replace(c.experience, chr(10), '<br/>')) as merged_experience
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c on c.id = m.candidate_id
	where 1=1
	and nullif(c.experience, '') is not NULL
)

, experience_group as (select master as master_candidate_id
	, string_agg(merged_experience, '<br/>' || '<br/>') as experience_group
	from merged_experience
	group by master
	) --select * from experience_group
	

/* AUDIT MERGED WORK SUMMARY
select c.id
, c.experience
, concat_ws('<br/>' || '<br/>', nullif(replace(c.experience, chr(10), '<br/>'), ''), '<br/>' || n.experience_group) as new_exp
from candidate c
join experience_group n on n.master_candidate_id = c.id
*/


update candidate c
set experience = concat_ws('<br/>' || '<br/>', nullif(replace(c.experience, chr(10), '<br/>'), ''), '<br/>' || n.experience_group)
from experience_group n
where n.master_candidate_id = c.id --38 rows