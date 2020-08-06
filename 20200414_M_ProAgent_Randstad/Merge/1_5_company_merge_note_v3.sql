--Merged company notes
with merged_notes as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.com_ext_id
	, concat_ws(chr(10), ('【Merged from PA: ' || m.com_ext_id || '】') , c.note) as merged_notes
	from mike_tmp_company_dup_check m
	left join company c on c.id = m.vc_pa_company_id
	where 1=1
	and c.note is not NULL
)
/*
select * from merged_notes
order by vc_company_id
*/
, notes_group as (select vc_company_id
	, string_agg(merged_notes, chr(10) || chr(13)) as notes_group
	from merged_notes
	group by vc_company_id
	) --select * from notes_group --4479 rows

/* AUDIT MERGED NOTE
select c.id
, c.note
, concat_ws(chr(10) || chr(13), '【Existing notes】' || chr(10) || nullif(c.note, ''), n.notes_group)
from company c
join notes_group n on n.vc_company_id = c.id --vc_company_id = 15326 | 'CPY020001'
*/

update company c
set note = concat_ws(chr(10) || chr(13), '【Existing notes】' || chr(10) || nullif(c.note, ''), n.notes_group)
from notes_group n
where n.vc_company_id = c.id