select *
from mike_tmp_candidate_dup_check

--UPDATE CANDIDATE NOTE
update candidate
set note = replace(replace(note, '\u3010', '【'), '\u3011', '】')
where id in (select distinct vc_candidate_id from mike_tmp_candidate_dup_check)

select id
, note
, replace(replace(note, '\u3010', '【'), '\u3011', '】')
from candidate
where id in (select distinct vc_candidate_id from mike_tmp_candidate_dup_check)


--UPDATE CANDIDATE WORK HISTORY SUMMARY
update candidate
set experience = replace(replace(experience, '\u3010', '【'), '\u3011', '】')
where id in (select distinct vc_candidate_id from mike_tmp_candidate_dup_check)


select id
, experience_details_json
, replace(replace(experience_details_json, '\u3010', '【'), '\u3011', '】')
from candidate
where id in (select distinct vc_candidate_id from mike_tmp_candidate_dup_check)
and experience_details_json is not NULL