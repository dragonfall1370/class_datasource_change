with test as (select peo_no as 'candidate_external_id',
job_no as 'job_external_id',
case when a.wip_status = 4 then 'OFFERED' ---PLACEMENT PERMANENT
when a.wip_status = 6 then 'OFFERED'
when a.wip_status = 9 then 'SHORTLISTED'
when a.wip_status = 10 then 'FIRST_INTERVIEW'
when a.wip_status = 11 then 'SHORTLISTED'
when a.wip_status = 13 then 'SENT'
when a.wip_status = 18 then 'SHORTLISTED'
when a.wip_status = 100 then 'SHORTLISTED'
when a.wip_status = 101 then 'SENT'
when a.wip_status = 102 then 'FIRST_INTERVIEW'
when a.wip_status = 103 then 'SHORTLISTED'
when a.wip_status = 104 then 'SECOND_INTERVIEW'
when a.wip_status = 105 then 'FIRST_INTERVIEW'
when a.wip_status = 109 then 'OFFERED'
when a.wip_status = 110 then 'OFFERED'
when a.wip_status = 111 then 'SHORTLISTED'
when a.wip_status = 112 then 'SECOND_INTERVIEW'
when a.wip_status = 113 then 'SECOND_INTERVIEW'
when a.wip_status = 115 then 'SECOND_INTERVIEW'
when a.wip_status = 121 then 'SENT'
when a.wip_status = 122 then 'SENT'
when a.wip_status = 123 then 'SHORTLISTED'
when a.wip_status = 124 then 'SHORTLISTED'
when a.wip_status = 125 then 'SHORTLISTED'
when a.wip_status = 126 then 'SHORTLISTED'
when a.wip_status = 127 then 'SENT'
when a.wip_status = 128 then 'SHORTLISTED'
else '' end as 'application-stage', 
cast(replace(replace(wip_status_date,left(wip_status_date,4),concat(left(wip_status_date,4),'-')),left(replace(wip_status_date,left(wip_status_date,4),concat(left(wip_status_date,4),'-')),7),
concat(left(replace(wip_status_date,left(wip_status_date,4),concat(left(wip_status_date,4),'-')),7),'-')) as datetime) as status_date
,ROW_NUMBER() over (partition by peo_no,job_no order by wip_status_date desc) as rn
from wip a
)

select *
from test where [application-stage] <> ''  and rn = 1
