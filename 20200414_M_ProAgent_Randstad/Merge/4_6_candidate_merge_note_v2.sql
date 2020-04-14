--Add PA[登録経路] Registration route and PA[認知経路] Cognitive pathway to Brief, and set “重複/Duplicate” in CF [Cognitive pathway]
with cognitive_value as (select cffv.form_id as join_form_id
		, cffv.field_id as join_field_id
		, cfl.translate as join_field_translate
		, cffv.field_value as join_field_value
		from configurable_form_language cfl
		left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
		where cfl.language = 'en' --input language
		and cffv.field_id = 11302 --Cognitive pathway
)		

, cognitive as (select a.additional_id as vc_pa_candidate_id --only candidates from PA
		, a.field_value
		, c.join_field_translate
		from additional_form_values a
		left join cognitive_value c on c.join_field_value = a.field_value
		where a.field_id = 11302
)

, merged_notes as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.cand_ext_id
	, concat_ws('<br>'
		, ('【Merged from PA: ' || m.cand_ext_id || '】')
		, (c.note || '<br>')
		, ('【登録経路】' || cs."name")
		, ('【認知経路】' || co.join_field_translate)
		) as merged_notes
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	left join cognitive co on co.vc_pa_candidate_id = m.vc_pa_candidate_id
	left join candidate_source cs on cs.id = c.candidate_source_id
	where 1=1
	and c.note is not NULL
)

, notes_group as (select vc_candidate_id
	, string_agg(merged_notes, '<br>' || '<br>') as notes_group
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
set note = concat_ws('<br>' || '<br>', c.note, n.notes_group)
from notes_group n
where n.vc_candidate_id = c.id