select * from (
        select
        #vacid as 'application-positionExternalId'
        concat(vacid,"copperq") as 'application-positionExternalId'
        #, canid as 'application-candidateExternalId'
        , concat(CanID,"copperq") as 'application-candidateExternalId'
        , case
                when accepted = 1 then 'PLACED'
                when offer = 1 then 'OFFERED'
                when interview = 1 then '1ST_INTERVIEW'
                when cvsent = 1 then 'SENT'
                when contacted = 1 then 'SHORTLISTED'
                end as 'Stage'
        # select *
        from copperq.shortlist
        order by vacid desc ) t
where Stage is not null