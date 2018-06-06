with JobApplication as (select VacancyUniqID, CandidateUniqID, max(UniqueID) as maxAppActionId
 from Matches
 where VacancyUniqID > 0 and status in ('DEC','DWO','FINT','FOJ','MAT','NI','NS','RSL','CVS','1st','2nd','OFF','PLA','PREC','1TI','3rd') --| based on job application mapping
 group by VacancyUniqID, CandidateUniqID)

select concat('FR',ja.VacancyUniqID) as 'application-positionExternalId',
	concat('FR',ja.CandidateUniqID) as 'application-candidateExternalId',
	Status,
	case
		when status in ('DEC','DWO','FINT','FOJ','MAT','NI','NS','RSL') then 'SHORTLISTED'
		when status in ('CVS','PREC') then 'SENT'
		when status in ('1st','1TI') then 'FIRST_INTERVIEW'
		when status in ('2nd','3rd') then 'SECOND_INTERVIEW'
		when status in ('OFF') then 'OFFERED'
		when status in ('PLA') then 'PLACED'
		else '' END AS 'application-stage'
from JobApplication ja left join Matches m on ja.maxAppActionId = m.UniqueID

--total: 13445