--->> ACTIVE | AVAILABILITY
with latest_candidate as (select m.*
	, c.active as active
	, c.availability as availability
	, c2.active as master_active
	, c2.availability as master_availability
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c on m.candidate_id = c.id
	left join candidate c2 on m.master = c2.id
	where 1=1
	and rn = 1 --already get latest candidate to update
) --select  * from latest_candidate where availability > master_availability

--Update after application merged
update candidate c
set availability = lc.pa_availability
from latest_candidate lc
where lc.vc_candidate_id = c.id
and (availability > master_availability)


update candidate c
set active = lc.pa_active
from latest_candidate lc
where lc.vc_candidate_id = c.id
and (active > master_active)


--->> DOB | dup no need to update


--->> CANDIDATE SOURCE CHECK | NOT UPDATE
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.candidate_source_id as slave_candidate_source
	, cs1.name as slave_source
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.candidate_source_id as master_candidate_source
	, cs2.name as master_source
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate_source cs1 on cs1.id = c1.candidate_source_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	left join candidate_source cs2 on cs2.id = c2.candidate_source_id
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
)

update candidate
set candidate_source_id = lc.slave_candidate_source
from latest_candidate lc
where lc.master_candidate_id = candidate.id


--->> PRIMARY PHONE / MOBILE / WORK EMAIL
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.phone as slave_phone
	, c1.phone2 as slave_mobile
	, c1.home_phone as slave_home_phone
	, c1.work_email as slave_work_email
	, c1.skype as slave_skype
	, c1.linked_in_profile as slave_linked_in_profile
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.phone as master_phone
	, c2.phone2 as master_mobile
	, c2.home_phone as master_home_phone
	, c2.work_email as master_work_email
	, c2.skype as master_skype
	, c2.linked_in_profile as master_linked_in_profile
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) --select * from latest_candidate

update candidate c
set phone = case when phone is NULL or phone = '' then lc.slave_phone else phone end
, phone2 = case when phone2 is NULL or phone2 = '' then lc.slave_mobile else phone2 end
, work_email = case when work_email is NULL or work_email = '' then lc.slave_work_email else work_email end
from latest_candidate lc
where lc.master_candidate_id = c.id


--->> WORKING STATE
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.working_state as slave_working_state
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.working_state as master_working_state
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) --select * from latest_candidate


update candidate c
set working_state = lc.slave_working_state
from latest_candidate lc
where lc.master_candidate_id = c.id
and lc.slave_working_state is not NULL


--->> CURRENT SALARY | DESIRED SALARY | OTHER BENEFITS
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, nullif(c1.current_salary::text, '0') as slave_current_salary
	, nullif(c1.desire_salary::text, '0') as slave_desire_salary
	, nullif(c1.other_benefits, '') as slave_other_benefits
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, nullif(c2.current_salary::text, '0') as master_current_salary
	, nullif(c2.desire_salary::text, '0') as master_desire_salary
	, nullif(c2.other_benefits, '') as master_other_benefits
	, case 
			when nullif(c2.other_benefits, '') is NULL then ('【Merged from candidate: ' || m.candidate_id || '】') || chr(10) || nullif(c1.other_benefits, '')
			else concat_ws(chr(10), c2.other_benefits, ('【Merged from candidate: ' || m.candidate_id || '】') || chr(10) || c1.other_benefits) end as new_other_benefit
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) --select * from latest_candidate


update candidate c
set current_salary = case when current_salary is NULL then lc.slave_current_salary::float else current_salary end
, desire_salary = case when desire_salary is NULL then lc.slave_desire_salary::float else desire_salary end
, other_benefits = new_other_benefit
from latest_candidate lc
where lc.master_candidate_id = c.id


--->>CURRENT LOCATION
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.current_location_id as slave_current_location_id
	, cl1.address as slave_address
	, cl1.current_location_candidate_id as slave_current_location_candidate_id
	, cl1.personal_location_candidate_id as slave_personal_location_candidate_id
	, cl1.desired_location_candidate_id as slave_desired_location_candidate_id
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.current_location_id as master_current_location_id
	, cl2.address as master_address
	, cl2.current_location_candidate_id as master_current_location_candidate_id
	, cl2.personal_location_candidate_id as master_personal_location_candidate_id
	, cl2.desired_location_candidate_id as master_desired_location_candidate_id
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join common_location cl1 on cl1.id = c1.current_location_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	left join common_location cl2 on cl2.id = c2.current_location_id
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	and nullif(cl1.address, '') is not NULL
	order by m.candidate_id
) --select * from latest_candidate 

update candidate c
set current_location_id = lc.slave_current_location_id
from latest_candidate lc
where lc.master_candidate_id = c.id


--UPDATE current_location_candidate_id NULL for master candidates
update common_location c
set current_location_candidate_id = NULL
from latest_candidate lc
where lc.master_current_location_id = c.id


--UPDATE current_location_candidate_id
update common_location c
set current_location_candidate_id = lc.master_candidate_id
from latest_candidate lc
where lc.slave_current_location_id = c.id --current slave candidate location ID with master candidate ID


--->>MET/NOT MET
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.status as slave_status
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.status as master_status
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	and rn = 1
	--and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) --select * from latest_candidate where slave_status = 1 and master_status = 2 --update only MET status


update candidate c
set status = lc.slave_status
from latest_candidate lc
where lc.master_candidate_id = c.id
and slave_status = 1 and master_status = 2 --update only MET status