
--CUSTOM FIELD > Job Location City / Town
SELECT
          cj.JOB as additional_id, jg.JOB_ID as '#JOB_ID', jg.JOB_TITLE
        , 'add_job_info' as additional_type --
        , 1006 as form_id
        , 11266 as field_id
        --, jh.filled_dt as 'Date Filled', CONVERT(VARCHAR(5),jh.filled_tm,108) as 'Time Filled'
        , MN.description as field_value
        , 11266 as constraint_id
--select distinct jg.location_cd, MN.description, count(*)
from PROP_X_CLIENT_JOB cj  --8076 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE 
left join (select * from MD_MULTI_NAMES MN where LANGUAGE = 10010) MN on MN.ID = jg.location_cd
where MN.ID is not null
--where jg.JOB_ID = 880568
group by jg.location_cd, MN.description





-- CUSTOM FIELD > Date Filled
SELECT
          cj.JOB as additional_id, jg.JOB_ID as '#JOB_ID', jg.JOB_TITLE
        , 'add_job_info' as additional_type --
        , 1006 as form_id
        , 11265 as field_id
        --, jh.filled_dt as 'Date Filled', CONVERT(VARCHAR(5),jh.filled_tm,108) as 'Time Filled'
        , jh.filled_dt as field_value
        , 11265 as constraint_id
from PROP_X_CLIENT_JOB cj  --8076 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE
left join (select REFERENCE,FILLED_DT,FILLED_TM,NO_CV_SENT,NO_IV_ARR,NO_IV_ATT from PROP_JOB_HIST) jh on jh.REFERENCE = cj.JOB
where jh.filled_dt is not null
