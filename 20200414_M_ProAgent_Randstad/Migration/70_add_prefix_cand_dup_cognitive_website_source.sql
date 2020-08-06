--TEMP TABLE FOR DUPLICATE COGNITIVE CANDIDATES
CREATE TABLE mike_tmp_candidate_dup_cognitive_website
(candidate_id bigint
, candidate_owners character varying (1000)
, candidate_source character varying (1000)
, reg_date timestamp
, candidate_name character varying (1000)
)


select id, note
, '【2020/07/14 登録経路確認済　Source checked】' || '<br/>' || note as new_note
from candidate
where id in (select candidate_id from mike_tmp_candidate_dup_cognitive_website)


--MAIN SCRIPT
update candidate
set note = '【2020/07/14 登録経路確認済　Source checked】' || '<br/>' || note
where id in (select candidate_id from mike_tmp_candidate_dup_cognitive_website)