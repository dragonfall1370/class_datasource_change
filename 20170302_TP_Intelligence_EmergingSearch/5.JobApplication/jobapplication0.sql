select vacid as 'application-positionExternalId'
        , canid as 'application-candidateExternalId'
        , case
                when accepted = 1 then 'PLACED'
                when offer = 1 then 'OFFERED'
                when interview = 1 then '1ST_INTERVIEW'
                when cvsent then 'SENT'
                when contacted then 'SHORTLISTED'
                end as 'Stage'
from emergingsearch.shortlist
