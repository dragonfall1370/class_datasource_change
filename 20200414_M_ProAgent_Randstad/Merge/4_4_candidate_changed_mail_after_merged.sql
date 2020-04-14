--UPDATE UNIQUE CANDIDATE EMAILS
select id, email, overlay(email placing '' from 1 for length(external_id) + 1 ) as new_email
from candidate
where deleted_timestamp is NULL
and external_id ilike 'CDT%'
and id not in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check) --160196
and overlay(email placing '' from 1 for length(external_id) + 1 ) <> 'candidate@noemail.com' --150963

--MAIN SCRIPT
update candidate
set email = overlay(email placing '' from 1 for length(external_id) + 1 )
where deleted_timestamp is NULL
and external_id ilike 'CDT%'
and id not in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)
and overlay(email placing '' from 1 for length(external_id) + 1 ) <> 'candidate@noemail.com'