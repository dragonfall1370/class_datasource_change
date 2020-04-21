with 
--Candidate URLs (may include Linkedin)
candidateurl as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as candidateurl
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '8c7d16c4-125f-498b-b932-5465373a782b' --URLs
	and e.commvalue is not NULL
	)
--Switchboard | not having multiple switchboard
, candidate_switchboard as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as candidate_switchboard
		from person_eaddress pe
		left join eaddress e on e.ideaddress = pe.ideaddress
		where pe.idpersoncommunicationtype = 'dced2973-8162-4152-a75a-a7d7991d1577' --switchboard
		)

--MAIN SCRIPT FOR INJECTION
, cte_candidate AS (SELECT c.idperson candidate_id
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	, px.createdon::timestamp created_date
--Basic information
	, px.knownas preffered_name --#inject
	, case mrs.value
		when 'Married' then 2
		when 'Widowed' then 4
		when 'Partner' then 0
		when 'Seperated' then 5
		when 'Single' then 1
		when 'Divorced' then 3
		else 0 end as marital_stastus --#inject
--Communication info
	, px.phonehome home_phone --#ref
	, cw.candidate_switchboard switchboard --#ref
	, concat_ws(', ', nullif(px.phonehome, ''), nullif(cw.candidate_switchboard, '')) personal_phone --#inject
	, canu.candidateurl website --#inject
--Employment info
	, px.salary current_salary
	, px.minimumrequiredrate contract_rate
	, case when ut.value = 'Day' then 'DAYS' 
		when ut.value = 'Hour' then 'HOURS'
		else NULL end as contract_interval

	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
	LEFT JOIN candidate_switchboard cw on cw.idperson = p.idperson
	LEFT JOIN candidateurl canu on canu.idperson = p.idperson
	LEFT JOIN maritalstatus mrs on mrs.idmaritalstatus = px.idmaritalstatus_string
	LEFT JOIN unittype ut ON ut.idunittype = px.idunittype_string
)

select candidate_id cand_ext_id
, created_date as reg_date
, preffered_name
, marital_stastus
, personal_phone
, website
, contract_interval
from cte_candidate
where rn = 1