with jobapp as (
select JobHistory.candid,
JobHistory.jobid,
case when (JobHistory.status = 'CNDINTRST') then 'SHORTLISTED'
when (JobHistory.status = 'RECAGNT') then 'SENT'
when (JobHistory.status = 'INTPROC') then 'FIRST_INTERVIEW'
when (JobHistory.status = 'PLACEDJOB') then 'PLACEMENT_PERMANENT'
when (JobHistory.status = 'CNDPRSPCT') then 'SHORTLISTED'
when (JobHistory.status = 'CNDACCEPT') then 'SHORTLISTED'
when (JobHistory.status = 'LINKPROC') then 'SHORTLISTED'
when (JobHistory.status = 'CLNTOFFER') then 'OFFERED'
when (JobHistory.status = 'UNLINK') then 'SHORTLISTED'
when (JobHistory.status = 'CLNTINTRST') then 'SENT'
when (JobHistory.status = 'OFFERPROC') then 'OFFERED' else '' end
as 'stage' 
from JobHistory
where JobHistory.candid is not null),
appdup as (
select jobapp.candid,
jobapp.jobid,
jobapp.stage as 'stage',
case when (stage='SHORTLISTED') then '1'
when (stage='SENT') then '2'
when (stage='OFFERED') then '4'
when (stage='PLACEMENT_PERMANENT') then '5'
when (stage='FIRST_INTERVIEW') then '3' else '' end as 'stagenum'
from
jobapp where jobapp.stage <> ''),

jobfinal as (select candid, jobid, stage, row_number() over (partition by appdup.candid, appdup.jobid order by stagenum desc) as 'rownum' from appdup)

select candid as 'application-candidateExternalId', 
jobid as 'application-positionExternalId',
stage as 'application-stage' from jobfinal where rownum=1



