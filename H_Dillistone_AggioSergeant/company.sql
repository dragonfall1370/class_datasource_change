WITH cte_company_alias AS (
	SELECT 
		ca.*,
		ar.value alias_reason,
		ROW_NUMBER() OVER(PARTITION BY id_company ORDER BY ca.id_alias_reason, ca.alias_note, created_on DESC) rn
	FROM company_alias ca
	LEFT JOIN alias_reason ar ON ca.id_alias_reason = ar.id_alias_reason 
),
cte_parent_company AS (
	SELECT 
		DISTINCT id_company,
		company_name
	FROM company_x
),
cte_state AS (
	SELECT
		abbreviation,
		value
	FROM county_state
	WHERE value NOT IN ('Californian', 'Arkansasian', 'GeorgiaN')
),
cte_company AS (
	SELECT
	c.id_company company_id,
	TRIM(REPLACE(REPLACE(c.company_name,'LTd', 'Ltd'), 'Noble foods', 'Noble Foods')) company_name,
	cx.no_of_employees,
	ROW_NUMBER() OVER(PARTITION BY c.id_company ORDER BY c.id_company) rn_id,
	cpc.company_name parent_company,
	c.created_on,
	cx.company_switchboard switch_board,
	cx.default_phone phone,
	cx.default_fax fax,
	LEFT(TRIM(replace(cx.address_default_full, '\x0d\x0a', ', ')), strpos(TRIM(replace(cx.address_default_full, '\x0d\x0a', ', ')), ',') - 1) location_name,
	CASE
		WHEN RIGHT(TRIM(replace(cx.address_default_full, '\x0d\x0a', ', ')),1) = ',' THEN LEFT(TRIM(replace(cx.address_default_full, '\x0d\x0a', ', ')), CHAR_LENGTH(TRIM(replace(cx.address_default_full, '\x0d\x0a', ', '))) - 1) 
		ELSE TRIM(replace(cx.address_default_full, '\x0d\x0a', ', '))
	END address,
	cx.address_default_city city,
	CASE
		WHEN cx.address_default_county_state = 'WA' AND co.value = 'United States of America' THEN 'Washington' 
		WHEN cx.address_default_county_state = 'WA' AND co.value = 'Australia' THEN 'Western Australia'
		WHEN cx.address_default_county_state = 'NT' AND co.value = 'Canada' THEN 'Northwest Territories'
		WHEN cx.address_default_county_state = 'NT' AND co.value = 'Australia' THEN 'Northern Territory'
		ELSE cs.value
	END state,
	CASE
		WHEN co.value = 'United States of America' THEN 'US'
		WHEN co.value = 'United Kingdom' THEN 'GB'
		WHEN co.value = 'Canada' THEN 'CA'
		WHEN co.value = 'New Zealand' THEN 'NZ'
		WHEN co.value = 'Czech Republic' THEN 'CZ'
		WHEN co.value = 'Germany' THEN 'DE'
		WHEN co.value = 'Philippines' THEN 'PH'
		WHEN co.value = 'Spain' THEN 'ES'
		WHEN co.value = 'Australia' THEN 'AU'
		WHEN co.value = 'Belgium' THEN 'BE'
		WHEN co.value = 'Switzerland' THEN 'CH'
		WHEN co.value = 'China' THEN 'CN'
		WHEN co.value = 'Portugal' THEN 'PT'
		WHEN co.value = 'Hong Kong' THEN 'HK'
		WHEN co.value = 'Ukraine' THEN 'UA'
		WHEN co.value = 'Finland' THEN 'FI'
		WHEN co.value = 'Chile' THEN 'CL'
		WHEN co.value = 'Israel' THEN 'IL'
		WHEN co.value = 'Lithuania' THEN 'LT'
		WHEN co.value = 'Russia' THEN 'RU'
		WHEN co.value = 'Denmark' THEN 'DK'
		WHEN co.value = 'Japan' THEN 'JP'
		WHEN co.value = 'France' THEN 'FR'
		WHEN co.value = 'Singapore' THEN 'SG'
		WHEN co.value = 'Bulgaria' THEN 'BG'
		WHEN co.value = 'Norway' THEN 'NO'
		WHEN co.value = 'United Arab Emirates' THEN 'AE'
		WHEN co.value = 'India' THEN 'IN'
		WHEN co.value = 'Malta' THEN 'MT'
		WHEN co.value = 'Sweden' THEN 'SE'
		WHEN co.value = 'Italy' THEN 'IT'
		WHEN co.value = 'Luxembourg' THEN 'LU'
		WHEN co.value = 'Poland' THEN 'PL'
		WHEN co.value = 'Republic of Ireland' THEN 'IE'
		WHEN co.value = 'Austria' THEN 'AT'
		WHEN co.value = 'Iceland' THEN 'IS'
		WHEN co.value = 'South Korea' THEN 'KR'
		WHEN co.value = 'Algeria' THEN 'DZ'
		WHEN co.value = 'Estonia' THEN 'EE'
		WHEN co.value = 'Taiwan' THEN 'TW'
		WHEN co.value = 'Mexico' THEN 'MX'
		WHEN co.value = 'Hungary' THEN 'HU'
		WHEN co.value = 'Brazil' THEN 'BR'
		WHEN co.value = 'Slovakia' THEN 'SK'
		WHEN co.value = 'Argentina' THEN 'AR'
		WHEN co.value = 'Netherlands' THEN 'NL'
		WHEN co.value = 'Saudi Arabia' THEN 'SA'
		WHEN co.value = 'Samoa' THEN 'WS'
		WHEN co.value = 'Turkey' THEN 'TR'
	END country,
	cx.address_default_post_code postal_code,
	CASE
		WHEN LENGTH(cx.default_url) > 100 THEN SUBSTRING(cx.default_url FROM 1 FOR strpos(RIGHT(cx.default_url, LENGTH(cx.default_url) - 10), '/') + 9)
		WHEN strpos(cx.default_url, 'http') = 0 THEN concat('http://', cx.default_url) 
		ELSE cx.default_url
	END website,
	u.user_email company_owner,
-- 	d.new_document_name company_document,
	CONCAT_WS(
		E'\n',
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_id::TEXT, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company id: ', REPLACE(cx.company_id::TEXT, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_reference, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company reference: ', REPLACE(cx.company_reference, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.default_email, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Default company email: ', REPLACE(cx.default_email, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(coo.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company origin: ', REPLACE(coo.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(cr.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company rating: ', REPLACE(cr.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(css.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company status: ', REPLACE(css.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.client_relationship_notes, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company overview: ', E'\n', REPLACE(cx.client_relationship_notes, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_note, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company note: ', E'\n', REPLACE(cx.company_note,  '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_comment, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company comment: ', E'\n', REPLACE(cx.company_comment, '\x0d\x0a', ' ')) END
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.preferred_supplier, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Preferred supplier: ', REPLACE(cx.preferred_supplier, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.alert_text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Alert text: ', REPLACE(cx.alert_text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.branch, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Branch: ', REPLACE(cx.branch, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN cx.is_top_of_group = 0 THEN concat('Is top of group:NOT  No', E'\ 'Is top of group: Yes' END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.last_attachment_date, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Last attachment date: ', REPLACE(cx.last_attachment_date, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.group_name, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Group name: ', REPLACE(cx.group_name, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.owning_user, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Owning user: ', REPLACE(cx.owning_user, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_phone_other, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company phone other: ', REPLACE(cx.company_phone_other, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_email1, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company email 1: ', REPLACE(cx.company_email1, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_email_other, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company email other: ', REPLACE(cx.company_email_other, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(l.id_location, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company location: ', REPLACE(l.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(col.off_limit_by, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Off limit by: ', REPLACE(col.off_limit_by, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(col.off_limit_note, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Off limit note: ', REPLACE(col.off_limit_note, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(olt.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Off limit type: ', REPLACE(olt.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cca.alias_reason, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Alias reason: ', REPLACE(cca.alias_reason, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cca.alias_note, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Alias note: ', REPLACE(cca.alias_note, '\x0d\x0a', ' ')) END
	) note
	
	FROM company c
	LEFT JOIN company_x cx ON c.id_company = cx.id_company
	LEFT JOIN cte_parent_company cpc ON cx.parent_id = cpc.id_company
	LEFT JOIN "user" u ON c.id_user = u.id_user
	LEFT JOIN country co ON cx.address_default_id_country_string = co.id_country
	LEFT JOIN cte_state cs ON cx.address_default_county_state = cs.abbreviation
	LEFT JOIN company_origin coo ON cx.id_company_origin_string = coo.id_company_origin
	LEFT JOIN company_rating cr ON cx.id_company_rating_string = cr.id_company_rating
	LEFT JOIN company_status css ON cx.id_company_status_string = css.id_company_status
-- 	LEFT JOIN "location" l ON cx.id_location_string = l.id_location
-- 	LEFT JOIN company_off_limit col ON cx.id_company = col.id_company
-- 	LEFT JOIN off_limit_type olt ON col.id_off_limit_type = olt.id_off_limit_type
-- 	LEFT JOIN cte_company_alias cca ON cx.id_company = cca.id_company AND cca.rn = 1 
	WHERE c.is_deleted = 0
),
-- remove duplicate company due to state
remove_dup_company AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY TRIM(REPLACE(REPLACE(company_name,'LTd', 'Ltd'), 'Noble foods', 'Noble Foods')) ORDER BY company_id) rn_name
	FROM cte_company
	WHERE rn_id = 1
)

--------------------------------------------------------- Main query -----------------------------------------------------------------

SELECT
	company_id "company-externalId",
	CASE
		WHEN rn_name <> 1 THEN concat(company_name, ' ', rn_name)
		ELSE company_name
	END "company-name",
	no_of_employees,
	parent_company "company-headQuarter",
	location_name "company-locationName",
	city "company-locationCity",
	state "company-locationState",
	country "company-locationCountry",
	address "company-locationAddress",
	postal_code "company-locationZipCode",
	switch_board "company-switchBoard",
	phone "company-phone",
	fax "company-fax",
	website "company-website",
	company_owner "company-owners",
	note "company-note"
FROM remove_dup_company