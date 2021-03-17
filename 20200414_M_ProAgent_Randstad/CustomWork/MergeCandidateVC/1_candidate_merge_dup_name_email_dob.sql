--TEMP TABLE FOR REQUESTED TO-BE-MERGED CANDIDATES
create table mike_tmp_candidate_dup_name_mail_dob
(candidate_id int,
email character varying (100),
first_name character varying (100),
last_name character varying (100),
dob date,
master_slave character varying (100),
notes character varying (100),
master int
)


--Check if Master / Slave correct
select m.*
, master.*
from mike_tmp_candidate_dup_name_mail_dob m
left join (select * from mike_tmp_candidate_dup_name_mail_dob where master_slave = 'Master') master on master.candidate_id = m.master
where m.master is not NULL
order by m.candidate_id


--[Temp] table for master / slave candidates
select *
, row_number() over(partition by master order by candidate_id desc) as rn
--into mike_tmp_candidate_dup_name_mail_dob_master_slave
from mike_tmp_candidate_dup_name_mail_dob
where master is not NULL
order by candidate_id



--Check reg_date / last_activity_date
select m.candidate_id
, c1.insert_timestamp as slave_reg_date
, ce1.last_activity_date as slave_last_activity_date
, m.master
, c2.insert_timestamp as master_reg_date
, ce2.last_activity_date as master_last_activity_date
from mike_tmp_candidate_dup_name_mail_dob_master_slave m
left join candidate c1 on c1.id = m.candidate_id
left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
left join candidate c2 on c2.id = m.master
left join candidate_extension ce2 on ce2.candidate_id = m.master
where m.master is not NULL
order by m.candidate_id
