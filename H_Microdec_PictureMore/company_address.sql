WITH main_company AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY name ORDER BY create_timestamp) rn
	FROM organisation
),
company_address AS (
	SELECT
		CASE 
			WHEN zipcode IS NOT NULL THEN ROW_NUMBER() OVER(PARTITION BY c.organisation_ref, zipcode ORDER BY CASE WHEN main_address = 'Y' THEN 1 ELSE 2 END, country) 
			ELSE 1
		END  AS rn,
		c.organisation_ref company_id,
		a.main_address,
		a.displayname location_name,
		a.post_town city,
		a.county_state state,
		a.zipcode,
		CASE
			WHEN a.country = 'United Kingdom' THEN 'GB'
			WHEN a.country = 'Ireland' THEN 'IE'
			WHEN a.country = 'New Zealand' THEN 'NZ'
			WHEN a.country = 'Australia' THEN 'AU'
			WHEN a.country = 'South Africa' THEN 'ZA'
			WHEN a.country = 'United States of America' THEN 'US'
			WHEN a.country = 'Belgium' THEN 'BE'
			WHEN a.country = 'Hong Kong' THEN 'HK'
			WHEN a.country = 'Spain' THEN 'ES'
			WHEN a.country = 'Luxembourg' THEN 'LU'
			WHEN a.country = 'Germany' THEN 'DE'
			WHEN a.country = 'Switzerland' THEN 'CH'
			WHEN a.country = 'France' THEN 'FR'
			WHEN a.country = 'Norway' THEN 'NO'
		END AS country,
		CONCAT_WS(
			', ',
			CASE WHEN RIGHT(TRIM(a.address_line_1), 1) = ',' THEN LEFT(TRIM(a.address_line_1), LENGTH(TRIM(a.address_line_1)) - 1) ELSE TRIM(a.address_line_1) END, 
			CASE WHEN RIGHT(TRIM(a.address_line_2), 1) = ',' THEN LEFT(TRIM(a.address_line_2), LENGTH(TRIM(a.address_line_2)) - 1) ELSE TRIM(a.address_line_2) END,
			CASE WHEN RIGHT(TRIM(a.address_line_3), 1) = ',' THEN LEFT(TRIM(a.address_line_3), LENGTH(TRIM(a.address_line_3)) - 1) ELSE TRIM(a.address_line_3) END,
			a.post_town,
			a.county_state,
			a.country
		) AS address,
		SUBSTRING(SUBSTRING(a."WGS84" FROM STRPOS(a."WGS84", '(') + 1 FOR LENGTH(a."WGS84") - 8) FROM 1 FOR STRPOS(SUBSTRING(a."WGS84" FROM STRPOS(a."WGS84", '(') + 1 FOR LENGTH(a."WGS84") - 8), ' '))::FLOAT longitude,
		SUBSTRING(SUBSTRING(a."WGS84" FROM STRPOS(a."WGS84", '(') + 1 FOR LENGTH(a."WGS84") - 8) FROM STRPOS(SUBSTRING(a."WGS84" FROM STRPOS(a."WGS84", '(') + 1 FOR LENGTH(a."WGS84") - 8), ' '))::FLOAT latitude
	FROM main_company c
	LEFT JOIN address a ON c.organisation_ref = a.organisation_ref
	WHERE COALESCE(address_line_1, address_line_2, address_line_3, post_town, county_state, zipcode, country) IS NOT NULL
	ORDER BY company_id,
					CASE
						WHEN main_address = 'Y' THEN 1
						ELSE 2
					END
)

SELECT *
FROM company_address
WHERE rn = 1