/*
--VC TEMP TABLE FOR JOB
CREATE TABLE mike_tmp_pa_job
(job_id bigint
, job_ext_id character varying (1000)
, job_title character varying (1000)
, reg_date timestamp
, job_status character varying (1000)
)

--PA MAIN SCRIPT
select [PANO ] job_ext_id
, convert(datetime, [登録日], 120) reg_date
, [ポジション名] job_title
, [募集状況] job_status
from csv_job

*/

--Update Sourcing Close date to 2020-07-12 if Close status and end_date > 2020-07-13
select pd.id, pd.name
, pd.head_count_open_date
, pd.head_count_close_date
, '2020-07-12 00:00:00' as close_date
, m.*
from position_description pd
join mike_tmp_pa_job m on m.job_id = pd.id
where m.job_status = 'Close'
and pd.head_count_close_date > '2020-07-13' --35589 jobs


--MAIN SCRIPT
update position_description pd
set head_count_close_date = '2020-07-12 00:00:00'
from mike_tmp_pa_job m
where m.job_id = pd.id
and m.job_status = 'Close'
and pd.head_count_close_date > '2020-07-13'