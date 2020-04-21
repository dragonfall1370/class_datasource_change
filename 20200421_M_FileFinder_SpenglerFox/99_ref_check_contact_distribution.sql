with --Selected users
selected_user as (select iduser, idorganizationunit, title, firstname, lastname
		, fullname, replace(useremail, 'spenglerfox.eu', 'spenglerfox.com') as useremail, createdon
		, isdisabled
		from "user"
		where 1=1
		--and isdisabled = '0'
		--and useremail ilike '%_@_%.__%'
		--and (firstname not ilike '%partner%' and jobtitle not ilike '%partner%')
		)

select distinct trim(campaigntitle) as group_name
, 1 as share_permission --1 public -2 private -3 share
, now() as insert_timestamp
, c.createdon
, c.createdby
, c.iduser
, u.useremail
, isdisabled
from campaign c
left join selected_user u on u.iduser = c.iduser
where 1=1
--and c.createdon::timestamp >= now() - interval '5 years'
and campaigntitle ilike '%geneva%'
--and fullname ilike '%zsuzsa%zimonyi%'
order by trim(campaigntitle)