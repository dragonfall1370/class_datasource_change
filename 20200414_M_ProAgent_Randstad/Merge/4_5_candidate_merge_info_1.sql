/* BACKUP CURRENT CANDIDATE
select *
into candidate_bkup_20200229
from candidate
where deleted_timestamp is NULL --241200
*/

--->>ACTIVE | AVAILABILITY
with latest_candidate as (select m.*
	, c.active as pa_active
	, c.availability as pa_availability
	from mike_tmp_candidate_dup_check m
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and coalesce(vc_pa_latest_date) > coalesce(vc_latest_date, '1900-01-01') --5668 rows | 5777 rows without rn=1
)

update candidate c
set active = lc.pa_active
, availability = lc.pa_availability
from latest_candidate lc
where lc.vc_candidate_id = c.id
and (lc.pa_active is not NULL or lc.pa_availability is not NULL)


--->>DOB
with latest_candidate as (select m.*
	, c.date_of_birth as pa_dob
	from mike_tmp_candidate_dup_check m
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and coalesce(vc_pa_latest_date) > coalesce(vc_latest_date, '1900-01-01') --5668 rows
)

update candidate c
set date_of_birth = lc.pa_dob
from latest_candidate lc
where lc.vc_candidate_id = c.id
and lc.pa_dob is not NULL

--->>CANDIDATE SOURCE
with latest_candidate as (select m.vc_candidate_id
	, vc_latest_date
	, m.vc_pa_candidate_id
	, vc_pa_latest_date
	, m.rn
	, c.candidate_source_id as pa_candidate_source_id
	, cs.name as candidate_source_name
	--, c2.candidate_source_id as vc_candidate_source_id
	--, cs2.name as vc_candidate_source_name
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	join candidate_source cs on cs.id = c.candidate_source_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	--join candidate_source cs2 on cs2.id = c2.candidate_source_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_latest_date) > coalesce(vc_latest_date, '1900-01-01')
	and cs.id is not NULL
	) --5668

update candidate c
set candidate_source_id = lc.pa_candidate_source_id
from latest_candidate lc
where lc.vc_candidate_id = c.id
and lc.pa_candidate_source_id is not NULL


--->>PRIMARY PHONE | WORK EMAIL
with latest_candidate as (select m.*
	, c.phone as pa_phone
	, c.work_email as pa_work_email
	from mike_tmp_candidate_dup_check m
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5668 rows
	and (c.phone is not NULL or c.work_email is not NULL) --5408 rows
)

update candidate c
set phone = case when phone is NULL or phone = '' then lc.pa_phone else phone end
, work_email = case when work_email is NULL or work_email = '' then lc.pa_work_email else work_email end
from latest_candidate lc
where lc.vc_candidate_id = c.id


--->WORKING STATE
with latest_candidate as (select m.*
	, c.working_state as pa_working_state
	from mike_tmp_candidate_dup_check m
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5668 rows
	and c.working_state is not NULL --5165
	)

update candidate c
set working_state = lc.pa_working_state
from latest_candidate lc
where lc.vc_candidate_id = c.id


--->>CURRENT SALARY | DESIRED SALARY | OTHER BENEFITS
with latest_candidate as (select m.*
	, nullif(c.current_salary::text, '0') as pa_current_salary
	, nullif(c.desire_salary::text, '0') as pa_desire_salary
	, nullif(c.other_benefits, '') as pa_other_benefits
	from mike_tmp_candidate_dup_check m
	join candidate c on m.vc_pa_candidate_id = c.id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5668 rows
	and coalesce(nullif(c.current_salary::text, '0'), nullif(c.desire_salary::text, '0'), nullif(c.other_benefits, '')) is not NULL
	) --4945 rows

update candidate c
set current_salary = lc.pa_current_salary::float
, desire_salary = lc.pa_desire_salary::float
, other_benefits = lc.pa_other_benefits
from latest_candidate lc
where lc.vc_candidate_id = c.id


--->CURRENT LOCATION
with latest_candidate as (select m.*
	, c.current_location_id as pa_current_location_id
	--, c2.current_location_id
	, cl.id as pa_location_id
	, cl.address
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	join common_location cl on cl.id = c.current_location_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5635 rows
	)

update candidate c
set current_location_id = lc.pa_location_id
from latest_candidate lc
where lc.vc_candidate_id = c.id


--->>MET/ NOT MET
with latest_candidate as (select m.*
	, c.status as pa_status
	--, c2.status as current_status
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5635 rows
	and c.status = 1  --MET --2440 rows
	)

update candidate c
set status = lc.pa_status
from latest_candidate lc
where lc.vc_candidate_id = c.id


--->>MET/ NOT MET (no need to lastest)
with latest_candidate as (select m.*
	, c.status as pa_status
	--, c2.status as current_status
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	--join candidate c2 on c2.id = m.vc_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and c.status = 1  --MET --2440 rows
	)
	
update candidate c
set status = lc.pa_status
from latest_candidate lc
where lc.vc_candidate_id = c.id