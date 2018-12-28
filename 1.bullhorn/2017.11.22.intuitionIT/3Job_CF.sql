/*

with t as (
SELECT
          a.jobPostingID as additional_id 
        , a.title as 'position-title'
        , 'add_job_info' as additional_type
        , convert(int,1006) as form_id
        , convert(int,1018) as field_id
        , convert(varchar(max),
                case a.status
                when 'Accepting' then 2
                when 'Accepting candidates' then 3
                when 'Archive' then 4
                when 'Bid' then 5
                when 'Covered' then 6
                when 'Dead' then 7
                when 'Filled' then 8
                when 'Filled by Client' then 9
                when 'Filled by Competitor' then 10
                when 'Live' then 11
                when 'Lost to Competitor' then 12
                when 'On Hold' then 13
                when 'Placed' then 14       
                end) as field_value
-- select distinct status
from bullhorn1.BH_JobPosting a
where status <> ''
)
--select count(*) from t
select * from t where additional_id <> 138

*/


with t as (
SELECT
          a.jobPostingID as additional_id 
        , a.title as 'position-title'
        , 'add_job_info' as additional_type
        , convert(int,1006) as form_id
        , convert(int,1019) as field_id
        , dateEnd as field_date_value
-- select distinct status
from bullhorn1.BH_JobPosting a
where dateEnd <> '' --and dateEnd <> '30/01/1900 00:00:00'
)
--select count(*) from t
select * from t where additional_id = 7090


