
with 
ja0 as (
       -- INTERVIEW
       select
                c.contactid as 'application-candidateExternalId'
              , case when (ltrim(replace(c.firstname,'?','')) = '' or  c.firstname is null) then 'Firstname' else ltrim(replace(c.firstname,'?','')) end as 'candidate-firstName'
              , case when (ltrim(replace(c.lastname,'?','')) = '' or c.lastname is null) then concat('Lastname-',c.contactid) else ltrim(replace(c.lastname,'?','')) end as 'candidate-Lastname'       
              , i.vacancyid as 'application-positionExternalId'
              , i.job
              , c2.regdate as 'dateAdded'
              , coalesce(nullif(case
                           when c2.offered =1 then 'OFFERED'
                           when c2.accepted = 1 then 'OFFERED'
                           when c2.tobearranged = 1 then 'FIRST_INTERVIEW'
                           when c2.CVSent = 1 then 'SENT'
                           when c2.Interested = 1 then 'SHORTLISTED'
                           when c2.onhold = 1 then 'SHORTLISTED'
                           when c2.replacementneeded = 1 then 'SHORTLISTED'
                           else '' end, ''), '')  as 'application-stage'
              --, c2.CVSent, c2.tobearranged, c2.Interested, c2.onhold, c2.offered, c2.offerid, c2.accepted, /*c2.contactsource,  c2.NotInterested, c2.withdrawn, c2.rejected, c2.rejectedoffer,*/ c2.replacementneeded
       -- select count(*)
       from dbo.interviews i
       left join (select can.contactid, c.firstname, c.lastname  from dbo.candidates can left join dbo.contacts c on c.contactid = can.contactid where c.type in ('Candidate') ) c on c.contactid = i.candidateid
       left join dbo.candidateslist2 c2 on c2.contactid = i.candidateid
UNION
       -- PLACEMENT
       select 
              can.contactid as 'application-candidateExternalId'
              , case when (ltrim(replace(c.firstname,'?','')) = '' or  c.firstname is null) then 'Firstname' else ltrim(replace(c.firstname,'?','')) end as 'candidate-firstName'
              , case when (ltrim(replace(c.lastname,'?','')) = '' or c.lastname is null) then concat('Lastname-',c.contactid) else ltrim(replace(c.lastname,'?','')) end as 'candidate-Lastname'       
              --, p.candfirstname, p.candlastname
              , p.vacancyid as 'application-positionExternalId'
              , p.position
              , p.dateplaced  as 'dateAdded'
              , 'PLACEMENT_PERMANENT' as  'application-stage'
       from dbo.placements p
       left join dbo.candidates can on can.contactid = p.contactid
       left join dbo.contacts c on c.contactid = can.contactid
)

, ja1 ("application-positionExternalId","application-candidateExternalId","application-Stage","dateAdded", rn) as (
       SELECT 
              "application-positionExternalId"
              ,"application-candidateExternalId"
              ,"application-Stage"
              ,"dateAdded"
              , rn = ROW_NUMBER() OVER (PARTITION BY "application-positionExternalId","application-candidateExternalId","application-Stage" ORDER BY "application-positionExternalId" desc) 
       FROM ja0 
)


select "application-positionExternalId","application-candidateExternalId","application-Stage", "dateAdded", current_timestamp as 'TIME_REJECTED'
from ja1
where rn = 1 and [application-stage] <> '' and ( [application-positionExternalId] is not null and [application-candidateExternalId] is not null)
and [application-stage] = 'PLACEMENT_PERMANENT'
order by [application-positionExternalId]  asc,
    CASE [application-stage]
        WHEN 'PLACEMENT_PERMANENT' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END asc
    