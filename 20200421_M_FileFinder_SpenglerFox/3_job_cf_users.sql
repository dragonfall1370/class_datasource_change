--
--ALL ACTIVE USERS/PARTNERS
with all_user as (select '000' as iduser
	, '000' idorganizationunit
	, '' title
	, '' fullname
	, 'Please select' user_fullname
	, now() as createdon
	, 1 rn
	
	UNION ALL
	select iduser, idorganizationunit, title
	, fullname
	, trim(fullname) as user_fullname
	, createdon::timestamp
	, 2 rn
	from "user"
	where isdisabled = '0'
	and firstname not ilike 'Partner%' and fullname <> '. .'
	
	UNION ALL
	select iduser, idorganizationunit, title
	, fullname
	, trim(fullname) as user_fullname
	, createdon::timestamp
	, 3 rn
	from "user"
	where isdisabled = '0'
	and firstname ilike 'Partner%' and fullname <> '. .'
	)
	
select user_fullname
from all_user
order by rn, user_fullname --172 rows


--PARTNERS
with all_partner as (select iduser, idorganizationunit, title, fullname
	, replace(lastname, '- ', '') as lastname
	, createdon
	from "user"
	where isdisabled = '0'
	--and useremail ilike '%_@_%.__%'
	and firstname ilike '%partner%'
	order by lastname)
	
select distinct lastname
from all_partner
order by lastname --76 rows