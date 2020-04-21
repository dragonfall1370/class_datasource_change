/* Before injecttion, add those to VC DB
alter table company_location
add column location_ext_id character varying (100)
*/

--COMPANY ADDITIONAL LOCATION
select cp.idcompany as com_ext_id
, cp.idcompany_paddress --location_ext_id
, cp.idpaddress as location_ext_id --Using this for CONTACT WORK LOCATION Reference
, cat.value as address_type --Postal Address | Invoice
, case when cat.value = 'Postal Address' then 'MAILING_ADDRESS'
		when cat.value = 'Invoice' then 'BILLING_ADDRESS'
		else NULL end as location_type
, left(replace(replace(pa.fulladdress,'\x0d\x0a', ' '), '  ', ' '), 200) as location_address
, left(replace(replace(pa.fulladdress,'\x0d\x0a', ' '), '  ', ' '), 200) as location_name
, pa.city as location_city
, pa.postcode as location_postcode
, pa.idcountry
, ct.value as location_country
, case when ct.abbreviation = 'UK' then 'GB' 
				when ct.value = 'United States' then 'US'
				when ct.value = 'Russian Federation' then 'RU'
				when ct.value = 'Ireland' then 'IE'
				when ct.value = 'Serbia' then 'RS'
				when ct.value = 'United States of America' THEN 'US'
				when ct.value = 'United Kingdom' THEN 'GB'
				when ct.value = 'Canada' THEN 'CA'
				when ct.value = 'New Zealand' THEN 'NZ'
				when ct.value = 'Czech Republic' THEN 'CZ'
				when ct.value = 'Germany' THEN 'DE'
				when ct.value = 'Philippines' THEN 'PH'
				when ct.value = 'Spain' THEN 'ES'
				when ct.value = 'Australia' THEN 'AU'
				when ct.value = 'United States of America' THEN 'US'
				when ct.value = 'Republic of Ireland' THEN 'IE'
				when ct.value = 'India' THEN 'IN'
				when ct.value = 'Switzerland' THEN 'CH'
				when ct.value = 'Nigeria' THEN 'NG'
				when ct.value = 'Greece' THEN 'GR'
				when ct.value = 'France' THEN 'FR'
				when ct.value = 'Netherlands' THEN 'NL'
				when ct.value in ('Unknown', '--') THEN NULL
				else ct.abbreviation end as location_country_code
, ct.abbreviation as source_country_code
, replace(replace(pa.fulladdress,'\x0d\x0a', ' '), '  ', ' ') as location_note
from company_paddress cp
inner join paddress pa on pa.idpaddress = cp.idpaddress
left join companyaddresstype cat on cat.idcompanyaddresstype = cp.idcompanyaddresstype
left join country ct on ct.idcountry = pa.idcountry
--where cp.idcompany = '9f5a6c5d-7565-44c1-9a98-4e01e1f183d4'