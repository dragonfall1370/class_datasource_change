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