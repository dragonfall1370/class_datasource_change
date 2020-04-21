/*
alter table common_location
add column contact_id bigint
*/

--CONTACT CURRENT ADDRESS
select pp.idperson as con_ext_id
, pp.idpaddress as location_ext_id
, replace(replace(p.fulladdress,'\x0d\x0a', ' '), '  ', ' ') as location_address
, replace(replace(p.fulladdress,'\x0d\x0a', ' '), '  ', ' ') as location_name
, p.city
, p.postcode
, p.idcountry
, ct.value as location_country
, case when ct.abbreviation = 'UK' then 'GB' 
		when p.idcountry = 'United States' then 'US'
		when p.idcountry = 'Russian Federation' then 'RU'
		when p.idcountry = 'Ireland' then 'IE'
		when p.idcountry = 'Serbia' then 'RS'
		when p.idcountry = 'United Kingdom' THEN 'GB'
		when p.idcountry = 'Canada' THEN 'CA'
		when p.idcountry = 'New Zealand' THEN 'NZ'
		when p.idcountry = 'Czech Republic' THEN 'CZ'
		when p.idcountry = 'Germany' THEN 'DE'
		when p.idcountry = 'Philippines' THEN 'PH'
		when p.idcountry = 'Spain' THEN 'ES'
		when p.idcountry = 'Australia' THEN 'AU'
		when p.idcountry = 'United States of America' THEN 'US'
		when p.idcountry = 'Republic of Ireland' THEN 'IE'
		when p.idcountry = 'India' THEN 'IN'
		when p.idcountry = 'Switzerland' THEN 'CH'
		when p.idcountry = 'Nigeria' THEN 'NG'
		when p.idcountry = 'Greece' THEN 'GR'
		when p.idcountry = 'France' THEN 'FR'
		when p.idcountry = 'Netherlands' THEN 'NL'
		when ct.abbreviation in ('Unknown', '--') THEN NULL
		else ct.abbreviation end as location_country_code
from person_paddress pp
left join paddress p on p.idpaddress = pp.idpaddress
left join country ct on ct.idcountry = p.idcountry
where 1=1
and pp.idpersonaddresstype = '995053e4-641b-4c06-97a8-b3d4594467d7' --private address
and pp.isdefault = '1'