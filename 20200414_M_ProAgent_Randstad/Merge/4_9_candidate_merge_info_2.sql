---> COMPANY COUNTS
with latest_candidate as (select m.*
	, c.company_count as pa_company_count
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, vc_reg_date) --5668 rows
	and c.company_count is not NULL
	)

update candidate c
set company_count = lc.pa_company_count
from latest_candidate lc
where lc.vc_candidate_id = c.id

--->>TOEIC
with latest_candidate as (select m.*
	, c.toeic_score as pa_toeic_score
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and c.toeic_score is not NULL
	)

update candidate c
set toeic_score = lc.pa_toeic_score
from latest_candidate lc
where lc.vc_candidate_id = c.id



--->LANGUAGE SKILLS
with pa_candidate_lang as (select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.skill_details_json::jsonb) as pa_candidate_lang
	, c.skill_details_json
	from mike_tmp_candidate_dup_check m 
	left join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and c.skill_details_json is not NULL
	--and m.vc_candidate_id = 42215
	--and m.vc_pa_candidate_id = 195515
	
UNION ALL
select m.vc_pa_candidate_id
	, m.vc_candidate_id
	, m.cand_ext_id
	, jsonb_array_elements(c.skill_details_json::jsonb) as vc_candidate_lang
	, c.skill_details_json
	from mike_tmp_candidate_dup_check m 
	join candidate c on m.vc_candidate_id = c.id
	where 1=1
	and c.skill_details_json <> ''
	--and m.vc_candidate_id = 42215
)

, merged_new as (select vc_candidate_id
	, array_to_json(array_agg(distinct pa_candidate_lang)) as new_candidate_lang
	from pa_candidate_lang
	where pa_candidate_lang is not NULL
	group by vc_candidate_id)
	
--select * from merged_new --11306
--where vc_candidate_id = 42215

--MAIN SCRIPT
update candidate c
set skill_details_json = m.new_candidate_lang
from merged_new m
where m.vc_candidate_id = c.id
--and m.vc_candidate_id = 42215


-->>CANDIDATE NAME
with latest_candidate_firstname as (select m.*
	, c.first_name as pa_first_name
	--, c2.first_name as vc_first_name
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, vc_reg_date) --5668 rows
	)
	
, latest_candidate_lastname as (select m.*
	, c.last_name as pa_last_name
	--, c2.last_name as vc_last_name
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, vc_reg_date) --5668 rows
	)
	
, latest_cand_firstname_kana as (select m.*
	, c.first_name_kana as pa_first_name_kana
	--, c2.first_name_kana as vc_first_name_kana
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, vc_reg_date) --5668 rows
	)
	
, latest_cand_lastname_kana as (select m.*
	, c.last_name_kana as pa_last_name_kana
	--, c2.last_name_kana as vc_last_name_kana
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, vc_reg_date) --5668 rows
	)
	
--MAIN SCRIPT
update candidate c
set first_name = m.pa_first_name
from latest_candidate_firstname m
where m.vc_candidate_id = c.id

update candidate c
set first_name_kana = m.pa_first_name_kana
from latest_cand_firstname_kana m
where m.vc_candidate_id = c.id

update candidate c
set last_name = m.pa_last_name
from latest_candidate_lastname m
where m.vc_candidate_id = c.id

update candidate c
set last_name_kana = m.pa_last_name_kana
from latest_cand_lastname_kana m
where m.vc_candidate_id = c.id
