with merged_notes as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.external_id
	, concat_ws('<br/>', ('【Merged from PA: ' || m.external_id || '】') , c.note) as merged_notes
	from mike_tmp_candidate_dup_check m
	left join candidate c on c.id = m.vc_pa_candidate_id
	where 1=1
	and c.note is not NULL
)

, notes_group as (select vc_candidate_id
	, string_agg(merged_notes, '<br/>' || '<br/>') as notes_group
	from merged_notes
	group by vc_candidate_id
	)
	
/* AUDIT MERGED NOTE
select c.id
, c.note
, concat_ws('<br/>' || '<br/>', c.note, n.notes_group)
from candidate c
join notes_group n on n.vc_candidate_id = c.id --
*/

update candidate c
set note = concat_ws('<br/>' || '<br/>', c.note, n.notes_group)
from notes_group n
where n.vc_candidate_id = c.id