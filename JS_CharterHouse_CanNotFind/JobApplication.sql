with test as (
       select 
              trim(candidate) as CanExtId,
              trim(vacancy) as JobExtId,
              accept, taccept, toffer,
              case 
                     when offer = 'True' and accept <> '' then 'OFFERED'
                     when accept = 'True' then 'OFFERED'
                     when interview = 'True' and offer = '' and accept = '' then 'FIRST_INTERVIEW'
                     when cvsent = 'True' and interview = '' and offer = '' and accept = '' then 'SENT'
                     when intiv = 'True' and interview = '' and offer = '' and accept = '' and cvsent = '' then 'SHORTLISTED'
                     when interested = 'True' and interview = '' and offer = '' and accept = '' and cvsent = '' and intiv = '' then 'SHORTLISTED'
                     when contacted = 'True' and interview = '' and offer = '' and accept = '' and cvsent = '' and intiv = '' and interested = '' then 'SHORTLISTED'
                     end as 'Application-stage',
              cast(treject as datetime) as RejectedDate
       -- select *
       from shortlisted )

,test2 as (select * from test where accept = 'True')

select *,
       case when accept = 'True' then 301 end as position_candidate_status
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
              end as offer_position_type,
       cast(start_date as datetime) as startdate,
       cast(end_date as datetime) as enddate
from test2 a
left join vacancies b on a.JobExtId = b.code