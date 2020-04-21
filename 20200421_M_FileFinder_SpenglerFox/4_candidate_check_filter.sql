--CANDIDATE WITH FILTER 5 YEARS
with personactivity as (select idperson, createdon
	, row_number() over(partition by idperson order by createdon desc) as rn
	from activitylogentity
	where contextentitytype = 'Person')

, cand as (SELECT c.idperson
	, px.createdon::timestamp
	, c.modifiedon::timestamp
	, a.createdon::timestamp as activity_date
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON px.idperson = c.idperson
	JOIN (select * from person where isdeleted = '0') p ON p.idperson = c.idperson
	LEFT JOIN (select * from personactivity where rn=1) a on a.idperson = c.idperson
	where px.createdon::timestamp >= now() - interval '5 years' --| 103627
	or c.modifiedon::timestamp >= now() - interval '5 years' --| 474245
	or a.createdon::timestamp >= now() - interval '5 years'
	)
	
select *
from cand
where rn = 1 --474578 rows

--CANDIDATE NO FILTER
with cand as (SELECT c.idperson
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON px.idperson = c.idperson
	JOIN (select * from person where isdeleted = '0') p ON p.idperson = c.idperson)
	
select * from cand where rn = 1 --586657