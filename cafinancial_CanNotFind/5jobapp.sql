
with 
ja as (
       select 
                CVID as 'application-candidateExternalId'
              --, DateSent as 'Candidate Activity Notes'
              --, ConsultantID as 'Owners'
              , JobSpecID as 'application-positionExternalId'
              , 'SENT' as  'application-stage'
              , DateSent as 'application-associated_date'              
       -- select top 20 *
       from CVSent where JobSpecID <> 0
UNION ALL
       select
               i.CVID as 'application-candidateExternalId'
              --,i.ContactID as 'Contact External Id'
              --,i.InterviewDate as 'Candidate Activity Notes'
              --,i.ConsultantID as 'Owners'
              ,i.JobSpecID as 'application-positionExternalId'
              , case 
                     when i.Stage in ('Internal - Telephone') then 'SHORTLISTED'
                     when i.Stage in ('1st Interview') then 'FIRST_INTERVIEW'
                     when i.Stage in ('2nd Interview','3rd Interview','4th Interview') then 'SECOND_INTERVIEW'
                     else '' end as 'application-stage' --'1st or 2nd+ Interview'
              , i.InputDate as 'application-associated_date'
       -- select distinct Stage -- select top 20 *
       from Interviews i where i.JobSpecID <> 0
UNION ALL
       select
                CVID as 'application-candidateExternalId'
               , JobSpecID as 'application-positionExternalId'
              --,Candidate as 'application-candidateExternalId'
       --       ,StartDate as 'Placement Note / Candidate Activity Notes'
       --       ,EndDate as 'Placement Note / Candidate Activity Notes'
       --       ,Fee as 'Placement Note / Candidate Activity Notes'
       --       ,TotalSalary as 'Placement Note / Candidate Activity Notes'
              , 'PLACEMENT_PERMANENT' as  'application-stage'
              , InputDate as 'application-associated_date'
        -- select top 20 *
       from Placements where JobSpecID <> 0
)
--select * from ja


, ja2 as (
       select
                [application-candidateExternalId]
              , [application-positionExternalId]
              , [application-stage]
              , [application-associated_date]
              , ROW_NUMBER() OVER(PARTITION BY [application-candidateExternalId], [application-positionExternalId]
               ORDER BY [application-associated_date] desc,
                      CASE [application-stage]
                      WHEN 'PLACEMENT_PERMANENT' THEN 1
                      WHEN 'OFFERED' THEN 2
                      WHEN 'SECOND_INTERVIEW' THEN 3
                      WHEN 'FIRST_INTERVIEW' THEN 4
                      WHEN 'SENT' THEN 5
                      WHEN 'SHORTLISTED' THEN 6
                      END asc 
              ) AS rn 
       FROM ja 
       where [application-stage] <> '' and [application-positionExternalId] <> 0
)
select * from ja2 
--where [application-candidateExternalId] in (2147478918, 2147478400,2147407085, 2147406793, 2147406754, -2146356023 )
where rn  = 1
