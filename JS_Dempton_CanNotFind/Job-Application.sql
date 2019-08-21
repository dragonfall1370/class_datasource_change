--InterviewTab--select cand_first,cand_last,date_inter,time_inter,date_inter_end, RefType,RefLocation from refs where cid = '43263'



with test as (select reference as 'application-positionExternalId',
cid as 'application-candidateExternalId', 

  case 
  when (b.profilestatus_id in (18,26,27,19,3,29,31,28,32,30,24,8)) then 'SHORTLISTED'
  when (b.profilestatus_id = 5) then 'SENT'
  when (b.profilestatus_id in (13,25)) then 'FIRST_INTERVIEW'
  when (b.profilestatus_id in (22,23)) then 'OFFERED'
  when (b.profilestatus_id = 21) then 'OFFERED' --- placed already
  else '' end
as 'application-stage'
from job_prof a left join ProfileStatus b on a.ProfileStatus_ID = b.ProfileStatus_ID
),


test2 as (select *, ROW_NUMBER() over (partition by [application-candidateExternalId], [application-positionExternalId] order by [application-candidateExternalId]) as 'row_num' from test where [application-stage] <> '')

select * from test2 where row_num = 1 and [application-positionExternalId] <> 0 order by [application-candidateExternalId]

