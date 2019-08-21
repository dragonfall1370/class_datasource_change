create view location1 as 
	select a.id, 
ltrim(rtrim(concat(
if(a.shipping_address_street = '' or a.shipping_address_street,'',a.shipping_address_street)
, if(a.shipping_address_city = '' or a.shipping_address_city is NULL,'',concat(', ',a.shipping_address_city))
, if(a.shipping_address_state = '' or a.shipping_address_state is NULL,'',concat(', ',a.shipping_address_state))
, if(a.shipping_address_postalcode = '' or a.shipping_address_postalcode is NULL,'',concat(', ',a.shipping_address_postalcode))
, if(a.shipping_address_country = '' or a.shipping_address_country is NULL,'',concat(', ',a.shipping_address_country)))))
	as 'locationName'
	from accounts a
-- select * from location1
-- DUPLICATION REGCONITION
-- , dup as (SELECT a.id, a.name, count(*) AS rn 
--  FROM accounts a join accounts ac on a.name = ac.name
--  and a.id >= ac.id
--  group by a.name, a.id
--  )
--  select * from dup
--  MAIN SCRIPT
select
  concat('BNS_',a.id) as 'company-externalId'
, a.name as 'OriginalName'
, if(a.name = '' or a.name is null,concat('NoCompanyName-',a.id),a.name) as 'company-name'
, if(l.locationName = '' or l.locationName is NULL,'',ltrim(l.locationName)) as 'company-locationName'
, if(l.locationName = '' or l.locationName is NULL,'',ltrim(l.locationName)) as 'company-locationAddress'
, if(a.shipping_address_city = '' or a.shipping_address_city is NULL,'',a.shipping_address_city) as 'company-locationCity'
, if(a.shipping_address_state = '' or a.shipping_address_state is NULL,'',a.shipping_address_state) as 'company-locationState'
, if(a.shipping_address_postalcode = '' or a.shipping_address_postalcode is NULL,'',a.shipping_address_postalcode) as 'company-locationZipCode'
, if(a.shipping_address_street = '' or a.shipping_address_street is NULL,'','NL') as 'company-locationCountry'
, coalesce(a.phone_office,a.phone_alternate) as 'company-phone'
, left(a.website,99) as 'company-website'
, uiv.email_address as 'company-owner'
, left(Concat(
			'Company External ID: BNS_', a.id,char(10),
			if(a.date_entered is NULL,'',Concat(char(10), 'Date/time entered: ', a.date_entered, char(10))),
			if(a.created_by = '' or a.created_by is NULL,'',Concat(char(10), 'Created by: ', if(cu.first_name is null, cu.last_name, if(cu.first_name=cu.last_name,cu.first_name,concat(cu.first_name,' ',cu.last_name))), char(10))),
			if(a.assigned_user_id = '' or a.assigned_user_id is NULL,'',Concat(char(10), 'Company Owner: ', if(au.first_name is null, au.last_name, if(au.first_name=au.last_name,au.first_name,concat(au.first_name,' ',au.last_name))), char(10))),
			if(a.date_modified is NULL,'',Concat(char(10), 'Date/Time modified: ', a.date_modified, char(10))),
			if(a.description = '' or a.description is NULL,'',Concat(char(10), 'Description: ', a.description, char(10)))),32000)
			as 'company-note'
from accounts as a
				left join location1 l on a.id = l.id
				left join users cu on a.created_by = cu.id
				left join users au on a.assigned_user_id = au.id
				left join user_info_view uiv on a.assigned_user_id = uiv.id
UNION ALL
select 'BNS_9999999','','Default Company','','','','','','','','','','This is Default Company from Data Import'

