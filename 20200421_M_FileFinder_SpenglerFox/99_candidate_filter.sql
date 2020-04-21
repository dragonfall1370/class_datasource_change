/* ADDITIONAL REFERENCE RULE
- candidates created within 5 years
- candidates latest modified within 5 years
- candidates last activity date within 5 years
*/

with candidatefilter as (SELECT c.idperson
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	, c.createdon::timestamp as created_on
	, c.modifiedon::timestamp as modified_on
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
	where 1=1
	--and c.createdon::timestamp >= now() - interval '5 years'
	--or c.modifiedon::timestamp >= now() - interval '5 years'
)	
	
, activitydate as (select idperson, max(createdon::timestamp) as max_date
	from activitylogentity
	group by idperson
	)

select c.idperson
from candidatefilter c
left join activitydate a on c.idperson = a.idperson
where c.rn = 1
and (c.created_on >= now() - interval '5 years' or c.modified_on >= now() - interval '5 years' or a.max_date >= now() - interval '5 years') --474983 rows / 586657