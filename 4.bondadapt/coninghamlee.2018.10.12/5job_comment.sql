with 
t as (
       SELECT
         --, a.[1 Auto Ref Numeric] as 'Id'
         b.[4 Ref No Numeric] as 'candidateExternalId', [1 Name Alphanumeric] as 'candidatename'
       , c.[1 Job Ref Numeric] as 'positionExternalId'
       , [25 CVS Date Date] as shortlistedon ,[31 RejectDte Date] as rejectedon
       , s.description as 'Send Via'         
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
       -- select top 10 * --91493 -- select distinct act.Description
       FROM F13 as a
       left JOIN F01 as b on (b.UniqueID = a.[4 Candidate Xref] /*or b.[4 RefNumber Numeric] = a.[5 Cand id Numeric]*/)
       left JOIN F03 as c on c.UniqueID = a.[6 Job Id Xref]
       JOIN ( select * from codes where Codegroup = 94 ) as act on act.Code = a.[15 Last Actio Codegroup  94] --left([15 Last Actio Codegroup  94], 2)
       left join (SELECT * FROM CODES WHERE Codegroup = 2) s on s.code = a.[24 CVSendMeth Codegroup   2]
       where b.[4 Ref No Numeric] is NOT NULL and c.[1 Job Ref Numeric] is not null
       and (act.Description <> 'Contr Pl' and act.Description <> 'Perm Place')
       --and c.[1 Job Ref Numeric] in (4118, 15481, 7079,7044,7053,7056,7064)
)

select 
       positionExternalId
       , Stuff(  
                 Coalesce('Candidate: ' + NULLIF(candidatename, '') + char(10), '')
              + Coalesce('Shortlisted On: ' + NULLIF(shortlistedon, '') + char(10), '')
              + Coalesce('Rejected On: ' + NULLIF(rejectedon, ''), '')
              , 1, 0, '') as note
from t
       