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
		CONCAT_WS(
			', ',
			CASE WHEN a.zipcode IS NOT NULL THEN a.zipcode END,
			CASE WHEN a.post_town IS NOT NULL THEN a.post_town END,
			CASE WHEN a.county_state IS NOT NULL THEN a.county_state END,
			CASE WHEN a.country IS NOT NULL THEN CONCAT('(', cc.country_code, ')') END
		) AS location_name,
		a.post_town city,
		a.county_state state,
		a.zipcode,
		cc.country_code country,
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
	LEFT JOIN country_code cc ON a.country = cc.country_name
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
AND country IS NOT NULL