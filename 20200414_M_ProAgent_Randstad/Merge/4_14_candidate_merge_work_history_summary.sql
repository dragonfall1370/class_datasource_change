with merged_experience as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.cand_ext_id
	, concat_ws('<br/>', ('【Merged from PA: ' || m.cand_ext_id || '】') , c.experience) as merged_experience
	from mike_tmp_candidate_dup_check m
	left join candidate c on c.id = m.vc_pa_candidate_id
	where 1=1
	and c.experience is not NULL
)

, experience_group as (select vc_candidate_id
	, string_agg(merged_experience, '<br/>' || '<br/>') as experience_group
	from merged_experience
	group by vc_candidate_id
	)
	
/* AUDIT MERGED NOTE
select c.id
, c.experience
, concat_ws('<br/>' || '<br/>', '【Work History】' || '<br/>' || nullif(replace(c.experience, chr(10), '<br/>'), ''), '<br/>' || n.experience_group)
from candidate c
join experience_group n on n.vc_candidate_id = c.id
*/ --9838 rows

update candidate c
set experience = concat_ws('<br/>' || '<br/>', '【Work History】' || '<br/>' || nullif(replace(c.experience, chr(10), '<br/>'), ''), '<br/>' || n.experience_group)
from experience_group n
where n.vc_candidate_id = c.id

/* AUDIT
with latest_candidate as (select m.*
	, c.saved_filename as pa_saved_filename
	from mike_tmp_candidate_dup_check m
	join (select candidate_id, saved_filename from candidate_document
							where document_type = 'candidate_photo') c on c.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5668 rows
	and c.saved_filename is not NULL
	) 

select *
from candidate_document
where 1=1
--and document_type = 'candidate_photo'
and candidate_id in (select vc_candidate_id from latest_candidate)
and id < 208466
order by id, candidate_id
*/