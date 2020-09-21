
-- select * from PROP_PERSON_GEN where person_id in (203951,221109,220598)

with sl as (
       select
               job, CANDIDATE, SHORTLIST, DATE_SHORT, DESCRIPTION 
       -- select count(*) -- select DISTINCT DESCRIPTION -- select *
       from PROP_X_SHORT_CAND SHORT_CAND
       INNER JOIN PROP_SHORT_GEN SHORT_GEN ON SHORT_GEN.REFERENCE = SHORT_CAND.SHORTLIST 
       INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SHORT_GEN.status
       where MN.ID is not null and LANGUAGE = 10010
       --WHERE SHORT_CAND.CANDIDATE = <<PROP_PERSON_GEN.REFERENCE>>"
)


, ja0 as (
select
	  candidate as 'application-candidateExternalId'
	, job as 'application-positionExternalId'
	, case 
             when jobtype.DESCRIPTION in ('Contract','Lead Contract','Lead Temp','Temp Regular','Temp Shift') then 302
             when jobtype.DESCRIPTION in ('Direct','Lead Direct Job') then 301
	      end as 'jobtype'	
       , case 
              when sl.DESCRIPTION = 'Interview Arranged' then 'FIRST_INTERVIEW'
              when sl.DESCRIPTION = 'Interview Attended' then 'FIRST_INTERVIEW'
              when sl.DESCRIPTION = 'Interview Cancelled' then 'FIRST_INTERVIEW'-- > REJECTED'
              when sl.DESCRIPTION = 'Offer Accepted' then 'OFFERED'
              when sl.DESCRIPTION = 'Offer Rejected' then 'OFFERED'-- > REJECTED'
              when sl.DESCRIPTION = 'Placed By Us' then 'OFFERED'--'PLACEMENT_PERMANENT'
              when sl.DESCRIPTION = 'Rejected' then 'SHORTLISTED'-- > REJECTED'
              when sl.DESCRIPTION = 'Resume Sent' then 'SENT'
              when sl.DESCRIPTION = 'Shortlisted' then 'SHORTLISTED'
              when sl.DESCRIPTION = 'Unbooked' then 'SHORTLISTED'-- > REJECTED'
              when sl.DESCRIPTION = 'Under Offer' then 'OFFERED'
              end as 'application-Stage'
       , DATE_SHORT              
       -- SHORTLISTED, SENT, FIRST_INTERVIEW, SECOND_INTERVIEW, OFFERED, PLACEMENT_PERMANENT, PLACEMENT_CONTRACT, PLACEMENT_TEMP
from sl
left join (SELECT REFERENCE, string_agg( MN.DESCRIPTION, ',') as DESCRIPTION FROM PROP_JOB_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_JOB_GEN.JOB_TYPE where MN.ID is not null and LANGUAGE = 10010 GROUP BY REFERENCE) jobtype on jobtype.REFERENCE = sl.JOB
where job is not null
--order by candidate,job
)
--select top 200 * from ja0

, ja1 ("application-positionExternalId","application-candidateExternalId", jobtype, "application-Stage", DATE_SHORT, rn) as (
       SELECT 
              "application-positionExternalId"
              ,"application-candidateExternalId"
              , jobtype
              ,"application-stage"
              , DATE_SHORT
              , rn = ROW_NUMBER() OVER (PARTITION BY "application-positionExternalId","application-candidateExternalId"/*,"application-Stage" */
                     ORDER BY "application-positionExternalId" desc
                            , CASE [application-stage]
                            WHEN 'PLACEMENT_PERMANENT' THEN 1
                            WHEN 'OFFERED' THEN 2
                            WHEN 'SECOND_INTERVIEW' THEN 3
                            WHEN 'FIRST_INTERVIEW' THEN 4
                            WHEN 'SENT' THEN 5
                            WHEN 'SHORTLISTED' THEN 6
                            END asc
                            , DATE_SHORT desc )
       FROM ja0 
       --left join (select jobPostingID from bullhorn1.BH_JobPosting where isdeleted <> 1 and status <> 'Archive') job on job.jobPostingID = ja0.[application-positionExternalId]
       --left join (select candidateid from bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') candidate on candidate.candidateid = ja0.[application-candidateExternalId]
       where "application-Stage" not like 'CANDIDATE%' and "application-Stage" <> '' --and (job.jobPostingID is not null and candidate.candidateid is not null)
       )



--select [application-stage], count(*) from ja1 where rn = 1 group by [application-stage] 
select "application-positionExternalId","application-candidateExternalId", "application-Stage", convert(date,DATE_SHORT) as 'actioned_date', DATE_SHORT as 'TIME_REJECTED'
    , JobType as POSITIONCANDIDATE_status
    , 3 as OFFER_draft_offer --used to move OFFERED to PLACED in VC [offer]
    , 2 as INVOICE_status --used to update invoice status in VC [invoice] as 'active'
    , 1 as INVOICE_renewal_index --default value in VC [invoice]
    , 1 as INVOICE_renewal_flow_status --used to update flow status in VC [invoice] as 'placement_active'
    , 1 as INVOICE_valid
from ja1
where rn = 1 --and [application-stage] = 'PLACED' --and [#Candidate Name] like '%Freeman%'

-- PROP_JOB_HIST -->> placement date