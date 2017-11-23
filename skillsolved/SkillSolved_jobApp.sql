with JobAppMapping as (select concat('RC',vacancy_id) as A
, concat('RC',candidate_id) as B
, case
	when movement in ('Shortlisted','Called','Awaiting Resume','Phone Interview','F2F Interview','Candidate Rejected','Remove From Shortlist') then 1
	when movement in ('Sent to Client','Client Rejected') then 2
	when movement = '1st Cliient Interview' then 3
	when movement in ('2nd Client Interview','3rd Client Interview','4th Client Interview') then 4
	when movement in ('Offer Process','Offer Rejected / Declilned','Reference Check') then 5
	when movement = 'Hired' then 6
	else 1 end as C
, movement
from tblShortlist where vacancy_id in (select vac_id from tblVacancies where vac_show = 1 and contact_id in (select contact_id from tblContacts where contact_show = 1)))

, MaxApplication as 
(select A, B, max(C) as C from JobAppMapping
group by A, B)

select A as 'application-positionExternalId'
	, B as 'application-candidateExternalId'
	, case C
	when 7 then 'INVOICED'
	when 6 then 'PLACED'
	when 5 then 'OFFERED'
	when 4 then '2ND_INTERVIEW'
	when 3 then '1ST_INTERVIEW'
	when 2 then 'SENT'
	when 1 then 'SHORTLISTED'
	else NULL end as 'application-stage'
from MaxApplication