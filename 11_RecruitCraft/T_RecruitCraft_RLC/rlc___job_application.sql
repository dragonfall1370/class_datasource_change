with JobAppMapping as (select concat('RLC',vacancy_id) as A
, concat('RLC',candidate_id) as B
, case
	when movement in ('Shortlisted','Called','Awaiting Resume','Phone Interview', 'Candidate Rejected','Remove From Shortlist') then 1
	when movement in ('Sent to Client','Client Rejected', 'Rejected by Client') then 2
	when movement = '1st Client Interview' then 3
	when movement = 'Internal F2F Interview' then 3
	when movement = 'Internal Interview' then 3
	when movement = 'Internal Phone Interview' then 3
	when movement in ('2nd Client Interview','3rd Client Interview','4th Client Interview') then 4
	when movement in ('Offer Process','Offer Rejected / Declilned','Reference Check', 'Offer Process') then 5
	when movement = 'Hired' then 6
	else 1 end as C
, movement
from tblShortlist 
where vacancy_id in (select vac_id from tblVacancies where vac_show = 1 and contact_id in (select contact_id from tblContacts where contact_show = 1))
and candidate_id in (select cn_id from tblCandidate where cn_show='Y')
union
select concat('RLC',vac_id) as A
, concat('RLC',cn_id) as B
, 6 as C, 'Placement' as movement
from ViewPlacements
where PlacementStatus='Active'
and vac_id in (select vac_id from tblVacancies where vac_show = 1 and contact_id in (select contact_id from tblContacts where contact_show = 1))
and cn_id in (select cn_id from tblCandidate where cn_show='Y')
)

, MaxApplication as 
(select A, B, max(C) as C from JobAppMapping
group by A, B)

select A as 'application-positionExternalId'
	, B as 'application-candidateExternalId'
	, case C
	when 7 then 'ONBOARDING'
	when 6 then 'PLACEMENT_PERMANENT'
	when 5 then 'OFFERED'
	when 4 then 'SECOND_INTERVIEW'
	when 3 then 'FIRST_INTERVIEW'
	when 2 then 'SENT'
	when 1 then 'SHORTLISTED'
	else NULL end as 'application-stage'
from MaxApplication

/*
This field only accepts 
SHORTLISTED
SENT
FIRST_INTERVIEW
SECOND_INTERVIEW
OFFERED
PLACEMENT_PERMANENT
PLACEMENT_CONTRACT
PLACEMENT_TEMP
ONBOARDING
Other values will not be recognized.
*/