select uniqueid as con_ext_id
	, "100 contact codegroup  23"
	, "101 candidate codegroup  23"
	--, "109 cont addr alphanumeric" as location_name
	--, "109 cont addr alphanumeric" as location_address
	, replace(concat_ws(', '
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 1), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 2), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 3), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 4), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 5), ''), NULL)
		), ',,', '') as location_name
	, replace(concat_ws(', '
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 1), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 2), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 3), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 4), ''), NULL)
		, coalesce(nullif(split_part("109 cont addr alphanumeric", '~', 5), ''), NULL)
		), ',,', '') as location_address
	, split_part("109 cont addr alphanumeric", '~', 3) as town
	, split_part("109 cont addr alphanumeric", '~', 5) as org_country
	, "110 cont pstcd alphanumeric" as post_code
	, case split_part("109 cont addr alphanumeric", '~', 5) --multiple cases can be changed
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
		when 'South Africa ' then 'ZA'
		when 'France' then 'FR'
		when 'United Arab Emirates' then 'AE'
		when 'Malaysia' then 'MY'
	else NULL end as country
	from f01
	where nullif("109 cont addr alphanumeric", '') is not NULL
	and "100 contact codegroup  23" = 'Y'