  
  -------05_JOB APPLICATION---------145,704----------
with 
ja0 as
(
         SELECT 
         --, a.[1 Auto Ref Numeric] as 'Id'
           b.[4 Ref No Numeric] as 'application-candidateExternalId'
         , c.[1 Job Ref Numeric] as 'application-positionExternalId'
         , case act.Description
              when 'Contr Pl' then 'PLACED'
              when 'CV Sent' then 'SENT'
              when 'Int Feedbk' then 'FIRST_INTERVIEW'
              when 'Interview' then 'FIRST_INTERVIEW'
              when 'Perm Place' then 'PLACED'
              when 'Shortlist' then 'SHORTLISTED'
              when 'Temp Book' then 'SHORTLISTED'
              else '' end as 'application-Stage'
       --, convert(date,left([16 Lastactnda Date], 10), 103) as 'actioned-date' --associated_date
       -- select count(*) --91493 -- select distinct act.Description
       FROM F13 as a
       left JOIN F01 as b on (b.UniqueID = a.[4 Candidate Xref] /*or b.[4 RefNumber Numeric] = a.[5 Cand id Numeric]*/)
       left JOIN F03 as c on c.UniqueID = a.[6 Job Id Xref]
       JOIN ( select * from codes where Codegroup = 94 ) as act on act.Code = a.[15 Last Actio Codegroup  94] --left([15 Last Actio Codegroup  94], 2)
       where b.[4 Ref No Numeric] is NOT NULL and c.[1 Job Ref Numeric] is not null
)

, ja1 ("application-positionExternalId","application-candidateExternalId","application-Stage", rn) as (
       SELECT 
              "application-positionExternalId"
              ,"application-candidateExternalId"
              ,"application-Stage"
              , rn = ROW_NUMBER() OVER (PARTITION BY "application-positionExternalId","application-candidateExternalId","application-Stage" ORDER BY "application-positionExternalId" desc) 
       FROM ja0 where [application-stage] <> '')
--select * from ja0

select "application-positionExternalId","application-candidateExternalId","application-Stage"
from ja1
where rn = 1 and [application-stage] <> '' --and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
and [application-stage] = 'OFFERED'
order by [application-positionExternalId]  asc,
    CASE [application-stage]
        WHEN 'PLACED' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END asc

