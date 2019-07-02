WITH selected_company AS (
	SELECT
		ct.company_id
	FROM companies_tags ct
	JOIN alpha_tag alt ON ct.tag_id = alt.id
	WHERE alt.id = 477
),
company_address AS (
	SELECT
	ROW_NUMBER() OVER(PARTITION BY a.company_id, a.latitude, a.longitude ORDER BY a.company_id) rn,
	a.company_id,
	CONCAT_WS(',', a.addressLine1, a.addressLine2,a.addressLine3) location_name,
	COALESCE(a.city, a.region, townCity) city,
	a.county,
	COALESCE(REPLACE(a.countryCode, 'UK', 'GB'), REPLACE(a.countryUkCode, 'ENG', 'GB')) country_code,
	CONCAT_WS(
		', ',
		CASE WHEN NULLIF(a.addressLine1, '') IS NOT NULL THEN a.addressLine1 END,
		CASE WHEN NULLIF(a.addressLine2, '') IS NOT NULL THEN a.addressLine2 END,
		CASE WHEN NULLIF(a.addressLine3, '') IS NOT NULL THEN a.addressLine3 END,
		CASE WHEN NULLIF(a.city, '') OR NULLIF(a.region, '') IS NOT NULL THEN COALESCE(a.city, a.region, townCity) END,
		CASE WHEN NULLIF(a.county, '') IS NOT NULL THEN a.county END,
		CASE WHEN NULLIF(a.country, '') IS NOT NULL THEN a.country END
	) AS address,
	latitude,
	longitude,
	phoneNumber phone_number
	FROM alpha_company_address a
	JOIN selected_company sc ON a.company_id = sc.company_id
	WHERE a.company_id IS NOT NULL
)

SELECT *
FROM company_address
WHERE rn = 1
AND COALESCE(NULLIF(location_name, ''), city, county, country_code, NULLIF(address, ''), latitude, longitude, phone_number) IS NOT NULL
ORDER BY company_id
-- LIMIT 500