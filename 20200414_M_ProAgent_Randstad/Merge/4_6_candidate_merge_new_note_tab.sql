--Merged company notes
with merged_notes as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.cand_ext_id
	, cn.title
	, concat_ws('<br/>', ('【Merged from PA: ' || m.cand_ext_id || '】') , cn.note) as merged_notes
	from mike_tmp_candidate_dup_check m
	left join candidate_note cn on cn.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and cn.note is not NULL
)
/*
select * from merged_notes
order by vc_candidate_id
*/
, notes_group as (select vc_candidate_id, title
	, string_agg(merged_notes, '<br/>' || '<br/>') as notes_group
	from merged_notes
	group by vc_candidate_id, title
	) --select * from notes_group --11121 rows

--select * from candidate_note limit 10

insert into candidate_note(candidate_id, title, note, insert_timestamp)
select vc_candidate_id candidate_id
, title
, notes_group
, current_timestamp insert_timestamp
from notes_group