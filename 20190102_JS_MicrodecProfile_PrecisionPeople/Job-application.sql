--with test as (select b.opportunity_ref from placing a
--left join event b on a.event_ref = b.event_ref where b.opportunity_ref <> '')
--, test2 as (select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, a.position_ref from event a where 
--opportunity_ref in (select * from test) 
--and a.position_ref <> '')
--,test3 as (select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, b.person_ref from test2 a left join position b on a.position_ref = b.position_ref)
--select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, a.person_ref, b.last_name, b.first_name from test3 a left join person b on a.person_ref = b.person_ref
--order by a.opportunity_ref

----------------------
--with test2 as (select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, a.position_ref from event a where 
--opportunity_ref <> ''
--and a.organisation_ref in (select organisation_ref from event))
--,test3 as (select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, b.person_ref from test2 a left join position b on a.position_ref = b.position_ref)
--select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, a.person_ref, b.last_name, b.first_name from test3 a left join person b on a.person_ref = b.person_ref
--left join lookup g
--order by a.opportunity_ref





with test2 as (select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, a.position_ref from event a where 
opportunity_ref <> ''
and a.position_ref <> '')
,test3 as (select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, b.person_ref from test2 a left join position b on a.position_ref = b.position_ref)
,test4 as (select a.opportunity_ref, a.type, a.outcome, a.z_last_type, a.z_last_outcome, a.organisation_ref, a.person_ref, b.last_name, b.first_name from test3 a left join person b on a.person_ref = b.person_ref)

,test5 as (select opportunity_ref,
case when z_last_outcome <> '' then z_last_outcome
when z_last_outcome = '' then outcome
when z_last_outcome = '' and outcome = '' then ''
end as final_stage,
case when z_last_type <> '' then z_last_type
when z_last_type = '' then type
end as final_type,
person_ref
from test4)
,test6 as (select * from lookup where code_type = 157)
,test7 as (select * from lookup where code_type = 123)

,test8 as (select opportunity_ref,person_ref,final_stage,final_type, b.description, c.description as type from test5 a left join test6 b on a.final_stage = b.code
left join test7 c on a.final_type = c.code)

select opportunity_ref,
person_ref,
case 
when type = 'Added from Search' then 'SHORTLISTED'
when type = 'Candidate Check In Call' then 'SHORTLISTED'
when type = 'Candidate Further Info Call' then 'SHORTLISTED'
when type = 'Candidate Reg Call' then 'SHORTLISTED'
when type = 'Candidate text sent' then 'SHORTLISTED'
when type = 'Chase Lead Call' then 'SHORTLISTED'
when type = 'Client Process Call' then 'SHORTLISTED'
when type = 'Client Service Call' then 'SHORTLISTED'
when type = 'Contract offer' then 'OFFERED'
when type = 'CV sent' then 'SENT'
when type = 'Email CV sent' then 'SENT'
when type = 'Int 1 with client' then 'FIRST_INTERVIEW'
when type = 'Int 2 with client' then 'SECOND_INTERVIEW'
when type = 'Int other' then 'SECOND_INTERVIEW'
when type = 'Invoice' then 'OFFERED'
when type = 'Job advert response (BB)' then 'SHORTLISTED'
when type = 'Meeting with candidate' then 'FIRST_INTERVIEW'
when type = 'Permanent offer' then 'OFFERED'
when type = 'Placement' then 'OFFERED'
when type = 'Quick text' then 'SHORTLISTED'
when type = 'Target' then 'SHORTLISTED'
when type = 'Temp Start' then 'OFFERED'
end as final_stage
from test8