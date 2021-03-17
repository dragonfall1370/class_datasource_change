--MARK DELETED TIMESTAMP FOR SLAVE CANDIDATES
select id, insert_timestamp, deleted_timestamp, external_id
from candidate
where id in (select candidate_id from mike_tmp_candidate_dup_name_mail_dob_master_slave) --67


select id, insert_timestamp, deleted_timestamp, external_id
from candidate
where id in (select master from mike_tmp_candidate_dup_name_mail_dob_master_slave) --66


update candidate
set deleted_timestamp = current_timestamp
where id in (select candidate_id from mike_tmp_candidate_dup_name_mail_dob_master_slave)