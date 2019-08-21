with t as (
        select
                  CandidateID as 'application-candidateExternalId'
                , JobOpeningID as 'application-positionExternalId'
                --, CandidateStatus as 'application-stage'
                , case
                        when CandidateStatus = 'Hired' then 'PLACED'
                        when CandidateStatus = 'Converted - Employee' then 'PLACED'
                        when CandidateStatus = 'Offer-Declined' then 'OFFERED'
                        when CandidateStatus = 'Offer-Withdrawn' then 'OFFERED'
                        when CandidateStatus = 'Offer-Accepted' then 'OFFERED'
                        when CandidateStatus = 'To-be-Offered' then 'OFFERED'
                        when CandidateStatus = 'Offer-Made' then 'OFFERED'
                        when CandidateStatus = 'Interview-Scheduled' then '1ST_INTERVIEW'               
                        when CandidateStatus = 'Interview-in-Progress' then '1ST_INTERVIEW'
                        when CandidateStatus = 'Rejected-for-Interview' then '1ST_INTERVIEW'
                        when CandidateStatus = 'Submitted-to-client' then 'SENT'
                        when CandidateStatus = 'Rejected by client' then 'SENT'
                        when CandidateStatus = 'Associated' then 'SHORTLISTED'
                        when CandidateStatus = 'Rejected-Hirable' then 'SHORTLISTED'
                        when CandidateStatus = 'Rejected' then 'SHORTLISTED'
                        when CandidateStatus = 'On-Hold' then 'SHORTLISTED'
                        when CandidateStatus = 'New' then 'SHORTLISTED'
                        when CandidateStatus = 'Waiting-for-Evaluation' then 'SHORTLISTED'            
                        when CandidateStatus = 'Un-Qualified' then 'SHORTLISTED'
                        when CandidateStatus = 'No-Show' then 'SHORTLISTED'
                else '' end as 'application-stage'
        --select distinct candidatestatus        
        from associated )
--select [application-stage],count(*) from t group by [application-stage]
select * from t