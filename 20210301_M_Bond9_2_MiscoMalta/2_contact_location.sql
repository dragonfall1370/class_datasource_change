with address as (select uniqueid
	, "100 contact codegroup  23"
	, "101 candidate codegroup  23"
	, "109 cont addr alphanumeric"
	, "110 cont pstcd alphanumeric"
	, address
	, splitrn
	from f01, unnest(string_to_array("109 cont addr alphanumeric", '~')) with ordinality as a(address, splitrn)
	where nullif("109 cont addr alphanumeric", '') is not NULL
	)

select a.uniqueid as con_ext_id
, a."109 cont addr alphanumeric" as location_name
, a."109 cont addr alphanumeric" as location_address
, a3.address as town
, a5.address as org_country
, case a5.address --multiple cases can be changed
		when 'New York, United States' then 'US'
		when 'Malta' then 'MT'
		when 'Cyprus' then 'CY'
		when 'US' then 'US'
		when 'Pakistan' then 'PK'
		when 'United States' then 'US'
		when 'Italy' then 'IT'
		when 'Isle Of Man' then 'IM'
		when 'India' then 'IN'
		when 'Switzerland' then 'CH'
		when 'Sweden' then 'SE'
		when 'Malta ' then 'MT'
		when 'Saudi Arabia' then 'SA'
		when 'United Kingdom' then 'GB'
		when 'Germany' then 'DE'
		when 'Hong Kong' then 'HK'
		when 'UK' then 'GB'
		when 'Great Britain' then 'GB'
		when 'UNITED KINGDOM' then 'GB'
		when 'NIGERIA ' then 'NG'
		when 'Gibraltar' then 'GI'
		when 'Ireland' then 'IE'
		when 'London' then 'GB'
		when 'MALTA' then 'MT'
		when 'Ukraine' then 'UA'
		when 'Usa' then 'US'
		when 'Italia' then 'IT'
		when 'Spain' then 'ES'
		when 'malta' then 'MT'
		when 'Qatar ' then 'QA'
	else NULL end as country
, a."110 cont pstcd alphanumeric" as post_code
, current_timestamp as insert_timestamp
from address a
left join (select * from address where splitrn = 3) a3 on a3.uniqueid = a.uniqueid --town
left join (select * from address where splitrn = 5) a5 on a5.uniqueid = a.uniqueid --country
where a.splitrn = 1 --get full address
and a."100 contact codegroup  23" = 'Y'
--and a.uniqueid = '8081010180FE8180' --Eduardo Cano
--and a.uniqueid = '8081010180968080' --Alex Mercieca
--and a.uniqueid = '808101019E828080' --Ronald Bonnici