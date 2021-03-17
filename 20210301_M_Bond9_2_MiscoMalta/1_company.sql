with consultant as (select f.uniqueid, "57 perm cons xref"
	, f17."1 name alphanumeric" as consultant
	, f17."72 email add alphanumeric" as email
	from f02 f
	left join f17 on f17.uniqueid = f."57 perm cons xref"
	where "57 perm cons xref" is not NULL
	
	UNION
	select f.uniqueid, "4 temp cons xref"
	, f17."1 name alphanumeric"
	, f17."72 email add alphanumeric"
	from f02 f
	left join f17 on f17.uniqueid = f."4 temp cons xref"
	where "4 temp cons xref" is not NULL)
	
, com_owners as (select uniqueid
	, string_agg(distinct email, ',') as com_owners
	from consultant
	group by uniqueid)
	
, comp_name as (select uniqueid, "1 name alphanumeric", "22 shortname alphanumeric", "6 ref no numeric"
	, case
			when position('€' in "1 name alphanumeric") > 0 then coalesce(nullif("22 shortname alphanumeric", ''), 'Company ID-' || uniqueid)
			when "1 name alphanumeric" is NULL then 'Company ID-' || "6 ref no numeric"
			else trim("1 name alphanumeric") end as new_company_name
	from f02) --select * from comp_name where uniqueid = '80810201BD8C8080'
	
, dup as (select uniqueid, "1 name alphanumeric", "22 shortname alphanumeric", "6 ref no numeric", new_company_name
		, ROW_NUMBER() OVER(PARTITION BY lower(trim(new_company_name)) ORDER BY uniqueid desc) AS rn
	from comp_name)

, com_document as (select uniqueid
	, string_agg(right("relative document path", position(E'\\' in reverse("relative document path")) - 1), ',') as com_document
	from f02docs2
	group by uniqueid
	)

--MAIN SCRIPT
select c.uniqueid "company-externalId"
, case when dup.rn = 1 then dup.new_company_name
		else dup.new_company_name || ' ID-' || dup."6 ref no numeric" end as "company-name"
, c."25 address alphanumeric" as "company-locationAddress"
, c."25 address alphanumeric" as "company-locationName"
, split_part(c."25 address alphanumeric", '~', 3) as "company-locationCity"
, c."33 postcode alphanumeric" as "company-locationZipCode"
, case right(c."25 address alphanumeric", position('~' in reverse(c."25 address alphanumeric")) - 1) 
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
	else NULL end as "company-locationCountry"
, c."28 phone alphanumeric" as "company-phone"
, left(nullif(c."71 website alphanumeric", 'www.'), 100) as "company-website"
, concat_ws(chr(10)
		, coalesce('Ref No: ' || c."6 ref no numeric", NULL)
		, coalesce('Email: ' || nullif("69 e-mail alphanumeric", ''), NULL)
		) as "company-note"
, co.com_owners as "company-owners"
, cd.com_document as "company-document"
from f02 c
left join dup on dup.uniqueid = c.uniqueid
left join com_owners co on co.uniqueid = c.uniqueid
left join com_document cd on cd.uniqueid = c.uniqueid