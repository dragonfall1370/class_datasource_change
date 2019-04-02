with test as (select trim(candidate) as CanExtId,
trim(vacancy) as JobExtId, accept,
case when offer = 'True' and accept <> '' then 'OFFERED'
when accept = 'True' then 'OFFERED'
when interview = 'True' and offer = 'false' and accept = 'false' then 'FIRST_INTERVIEW'
when cvsent = 'True' and interview = 'false' and offer = 'false' and accept = '' then 'SENT'
when intiv = 'True' and interview = 'false' and offer = 'false' and accept = 'false' and cvsent = 'false' then 'SHORTLISTED'
when interested = 'True' and interview = 'false' and offer = 'false' and accept = 'false' and cvsent = 'false' and intiv = 'false' then 'SHORTLISTED'
when contacted = 'True' and interview = 'false' and offer = 'false' and accept = 'false' and cvsent = 'false' and intiv = 'false' and interested = 'false' then 'SHORTLISTED'
end as 'Application-stage',

case when offer = 'True' and accept <> 'false' then Toffer
when accept = 'True' then Taccept
when interview = 'True' and offer = 'false' and accept = 'false' then Tinterview
when cvsent = 'True' and interview = 'false' and offer = 'false' and accept = 'false' then TCVSent
when intiv = 'True' and interview = 'false' and offer = 'false' and accept = 'false' and cvsent = 'false' then Tintiv
when interested = 'True' and interview = 'false' and offer = 'false' and accept = 'false' and cvsent = 'false' and intiv = 'false' then Tintereste
when contacted = 'True' and interview = 'false' and offer = 'false' and accept = 'false' and cvsent = 'false' and intiv = 'false' and interested = 'false' then Tcont
end as 'Application-Date'
,

cast(treject as datetime) as RejectedDate
from shortlisted)

select *
from test
where [Application-stage] is not null