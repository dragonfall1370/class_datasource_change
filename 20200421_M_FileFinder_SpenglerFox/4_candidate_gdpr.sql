WITH cte_candidate AS (
	SELECT c.idperson
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	, px.idprocessingreason_string, px.processingreasonby, px.processingreasonon, px.idprocessingstatus_string, px.processingstatusby, px.processingstatuson
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
)

select c.idperson cand_ext_id
	, case 
		when pr.value = 'Consent' then 1 --consent givent
		when pr.value = 'Legitimate Interests' then 5 --Legitimate Interest
		--when pr.value = 'Not Required' then '' --unknown
		--when pr.value = 'LI Social Media' then '' --unknown
		when pr.value = 'Contractual Obligation' then 4 --contract
		else NULL end as portal_status
	, 1 explicit_consent --unknown
	, 3 exercise_right --other
	, 6 request_through --other
	, processingreasonon::TIMESTAMP as request_through_date
	, 6 obtained_through --other
	, processingstatuson::TIMESTAMP as obtained_through_date
	, 0 expire --no expire | 1-expire on
	--, '2024-05-29 00:00:00'::TIMESTAMP expire_date
	, -10 obtained_by
	, current_timestamp as insert_timestamp
from cte_candidate c
left join processingreason pr on pr.idprocessingreason = c.idprocessingreason_string
left join processingstatus ps on ps.idprocessingstatus = c.idprocessingstatus_string
where c.rn = 1
and coalesce(idprocessingreason_string, idprocessingstatus_string) is not NULL