with merged_notes as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.com_ext_id
	, concat_ws(chr(10), ('【Merged from PA: ' || m.com_ext_id || '】') , c.note) as merged_notes
	from mike_tmp_company_dup_check2 m
	left join company c on c.id = m.vc_pa_company_id
	where 1=1
	and vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)
	and c.note is not NULL
)
, notes_group as (select vc_company_id
	, string_agg(merged_notes, chr(10) || chr(13)) as notes_group
	from merged_notes
	group by vc_company_id
	) --1284 rows
	
update company c
set note = concat_ws(chr(10) || chr(13), c.note, n.notes_group)
from notes_group n
where n.vc_company_id = c.id