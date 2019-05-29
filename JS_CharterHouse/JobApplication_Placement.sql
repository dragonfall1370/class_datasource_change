with test as (
select 
       trim(candidate) as CanExtId,
       trim(vacancy) as JobExtId, accept, cast(taccept as datetime) as taccept, cast(toffer as datetime) as toffer,
       case 
              when offer = 'True' and accept <> 'False' then 'PLACED'
              when accept = 'True' then 'OFFERED'
              when interview = 'True' and offer = 'False' and accept = 'False' then 'FIRST_INTERVIEW'
              when cvsent = 'True' and interview = 'False' and offer = 'False' and accept = 'False' then 'SENT'
              when intiv = 'True' and interview = 'False' and offer = 'False' and accept = 'False' and cvsent = 'False' then 'SHORTLISTED'
              when interested = 'True' and interview = 'False' and offer = 'False' and accept = 'False' and cvsent = 'False' and intiv = 'False' then 'SHORTLISTED'
              when contacted = 'True' and interview = 'False' and offer = 'False' and accept = 'False' and cvsent = 'False' and intiv = 'False' and interested = 'False' then 'SHORTLISTED'
              end as 'Application-stage',
       case 
              when offer = 'True' and accept <> 'False' then Toffer
              when accept = 'True' then Taccept
              when interview = 'True' and offer = 'False' and accept = 'False' then Tinterview
              when cvsent = 'True' and interview = 'False' and offer = 'False' and accept = 'False' then TCVSent
              when intiv = 'True' and interview = 'False' and offer = 'False' and accept = 'False' and cvsent = 'False' then Tintiv
              when interested = 'True' and interview = 'False' and offer = 'False' and accept = 'False' and cvsent = 'False' and intiv = 'False' then Tintereste
              when contacted = 'True' and interview = 'False' and offer = 'False' and accept = 'False' and cvsent = 'False' and intiv = 'False' and interested = 'False' then Tcont
              end as 'Application-Date',
       cast(treject as datetime) as RejectedDate
from shortlisted)


--select *
--from test
--where [Application-stage] is not null order by [Application-Date] desc


,test2 as (select * from test where [Application-stage] = 'PLACED')

select *,
       case when [Application-stage] = 'PLACED' then 301 end as position_candidate_status
       , 1 as invoice_valid
       , 1 as invoice_renewal_index
       , 1 as invoice_renewal_flow_status
       , 2 as invoice_status
       , 3 as offer_draft_offer
       , 1 as offer_valid
       , case 
              when b.type = 'P' then 1
              when b.type = 'C' then 2
              when b.type = 'F' then 1
              when b.type = 'T' then 3
              else 1
              end as offer_position_type
       , cast(start_date as datetime) as startdate_final
       , cast(end_date as datetime) as enddate_final
from test2 a
left join vacancies b on a.JobExtId = b.code