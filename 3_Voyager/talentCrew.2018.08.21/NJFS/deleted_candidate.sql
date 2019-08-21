
select --top 10
         concat('NJFS', c.intCandidateId) as 'candidate-externalId'
         , bitdeleted = 1
-- select count(*) --22568-- select distinct rn.vchNationalityName -- select top 10 *
from dCandidate c
where bitdeleted = 1