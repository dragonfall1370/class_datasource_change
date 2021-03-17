--BKUP CURRENT CANDIDATE
select *
into mike_bkup_candidate_20200907
from candidate


--CANDIDATE WORK HISTORY
with cand_merged as (select distinct candidate_id from mike_tmp_candidate_dup_name_mail_dob_master_slave
	UNION ALL
	select distinct master from mike_tmp_candidate_dup_name_mail_dob_master_slave
	)
	
select *
into mike_bkup_candidate_work_history_partial_20200907
from candidate_work_history
where candidate_id in (select candidate_id from cand_merged)



--CANDIDATE NOTES
select *
into mike_bkup_candidate_note_20200908
from candidate_note


--CANDIDATE CUSTOM FIELDS
select *
into mike_bkup_additional_form_values_form_1139_20200908
from additional_form_values
where form_id = 1139


--CANDIDATE CUSTOM FIELD TABLE
select *
into mike_bkup_configurable_form_group_value_20200908
from configurable_form_group_value


--CURRENT JOB APP
select *
into mike_bkup_position_candidate_20200908
from position_candidate


--CURRENT JOB COUNTS
select id, first_name, last_name, total_jobs, insert_timestamp, external_id
into mike_candidate_total_jobs_bkup_20200908
from candidate


--CURRENT OFFER_PERSONAL_INFO
