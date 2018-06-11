with JobApplication as (select vacancy_uniq_id, candidate_uniq_id, max(unique_id) as maxAppActionId
 from candidatejob
 where vacancy_uniq_id <> 0
 group by vacancy_uniq_id, candidate_uniq_id)

select concat('GP',ja.vacancy_uniq_id) as 'application-positionExternalId',
	concat('GP',ja.candidate_uniq_id) as 'application-candidateExternalId',
	status,
	case 
		when status like 'CV Sent' then 'SENT'
		when status in ('1st Tel Interview','First Interview') then 'FIRST_INTERVIEW'
		when status in ('Third Interview','Second Interview','Fourth Interview') then 'SECOND_INTERVIEW'
		when status like 'Offer Made' then 'OFFERED'
		when status like 'Placed' then 'PLACED'
		else 'SHORTLISTED' END AS 'application-stage'
from JobApplication ja left join candidatejob cj on ja.maxAppActionId = cj.unique_id

--select distinct unique_id
--from candidatejob

--select unique_id from candidatejob

--select unique_id, candidate_uniq_id, vacancy_uniq_id, status
----distinct status
--from candidatejob where vacancy_uniq_id <> 0
----order by candidate_uniq_id,vacancy_uniq_id

--select distinct client_account_code from vacancies

--select * from vacancies where unique_id =0