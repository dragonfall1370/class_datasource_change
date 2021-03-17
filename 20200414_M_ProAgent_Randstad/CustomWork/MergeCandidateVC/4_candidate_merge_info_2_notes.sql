--->>NOTES
with merged_notes as (select m.master
	, m.candidate_id
	, concat_ws('<br/>'
		, ('【Merged from candidate: ' || m.candidate_id || '】')
		, (c.note || '<br/>')
		) as merged_notes
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	join candidate c on c.id = m.candidate_id
	where 1=1
	and c.note is not NULL
)

, notes_group as (select master as master_candidate_id
	, string_agg(merged_notes, '<br>' || '<br>') as notes_group
	from merged_notes
	group by master
	)
	
/* AUDIT MERGED NOTE
select c.id
, c.note
, concat_ws('<br/>' || '<br/>', '【Original notes】' || '<br/>' || nullif(c.note, ''), n.notes_group) as new_notes
from candidate c
join notes_group n on n.master_candidate_id = c.id
--where c.id = 125250
*/

update candidate c
set note = concat_ws('<br/>' || '<br/>', '【Original notes】' || '<br/>' || nullif(c.note, ''), n.notes_group)
from notes_group n
where n.master_candidate_id = c.id


/* AUDIT

with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.note as slave_notes
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.note as master_notes
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	and rn = 1
	--and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) --select * from latest_candidate

*/

--->>CANDIDATE NEW NOTE TAB
with merged_notes as (select m.candidate_id
	, m.master
	, cn.title
	, concat_ws('<br/>', ('【Original notes】') , cn.note) as merged_notes
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate_note cn on cn.candidate_id = m.master
	where 1=1
	and cn.note is not NULL

	UNION ALL

	select m.candidate_id
	, m.master
	, cn.title
	, concat_ws('<br/>', ('【Merged from candidate: ' || m.candidate_id || '】') , cn.note) as merged_notes
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate_note cn on cn.candidate_id = m.candidate_id
	where 1=1
	and cn.note is not NULL
)

, notes_group as (select master as master_candidate_id
	, title
	, string_agg(merged_notes, '<br/>' || '<br/>') as notes_group
	from merged_notes
	group by master, title
	) --select * from notes_group where master_candidate_id = 152302 --59 rows
	
/* AUDIT
select *
from candidate_note
where candidate_id = 152302
*/

--Using [update] instead of [insert] due to existing new note tab
update candidate_note cn
set note = n.notes_group
from notes_group n
where n.master_candidate_id = cn.candidate_id
and n.title = cn.title
--and n.master_candidate_id != 152302


--->>COMPANY COUNTS
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.company_count as slave_company_count
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.company_count as master_company_count
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) --select * from latest_candidate where slave_company_count > master_company_count --8


update candidate c
set company_count = lc.slave_company_count
from latest_candidate lc
where lc.master_candidate_id = c.id
and lc.slave_company_count > lc.master_company_count


--->>TOEIC
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.toeic_score as slave_toeic_score
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.toeic_score as master_toeic_score
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) --select * from latest_candidate where nullif(slave_toeic_score, '') is not NULL


update candidate c
set toeic_score = lc.slave_toeic_score
from latest_candidate lc
where lc.master_candidate_id = c.id
and nullif(lc.slave_toeic_score, '') is not NULL



--->LANGUAGE SKILLS
with slave_candidate_lang as (select m.candidate_id
	, m.master
	, jsonb_array_elements(c.skill_details_json::jsonb) as slave_candidate_lang
	, c.skill_details_json
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	left join candidate c on c.id = m.candidate_id
	where 1=1
	and c.skill_details_json is not NULL
	--and m.vc_candidate_id = 42215
	--and m.vc_pa_candidate_id = 195515
	
UNION ALL
select m.candidate_id
	, m.master
	, jsonb_array_elements(c.skill_details_json::jsonb) as slave_candidate_lang
	, c.skill_details_json
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m 
	left join candidate c on c.id = m.master
	where 1=1
	and c.skill_details_json <> ''
	--and m.vc_candidate_id = 42215
)

, merged_new as (select master as master_candidate_id
	, array_to_json(array_agg(distinct slave_candidate_lang)) as new_candidate_lang
	from slave_candidate_lang
	where slave_candidate_lang is not NULL
	group by master) --select * from merged_new
	

update candidate c
set skill_details_json = m.new_candidate_lang
from merged_new m
where m.master_candidate_id = c.id