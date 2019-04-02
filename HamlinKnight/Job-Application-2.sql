with test as (select a.event_ref,a.opportunity_ref,a.organisation_ref,b.person_ref, c.description ,a.event_date,a.displayname,a.notes,a.z_last_type,z_last_outcome 
from event a right join event_role b on a.event_ref = b.event_ref
left join lookup c on b.type = c.code
where c.code_type = 124)

,test2 as (select a.*, b.description as 'type',event_date as 'start_date' from test a
left join lookup b on a.z_last_type = b.code
where person_ref <> ''
and b.code_type = 123)


,test3 as (select opportunity_ref,
person_ref, start_date,
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
from test2 where opportunity_ref <> '')

,test4 as (select *,row_number() over (partition by opportunity_ref,person_ref order by start_date desc) as rn from test3 where final_stage is not null)

select * from test4 where rn = 1 and opportunity_ref in (Select opportunity_ref from opportunity)
