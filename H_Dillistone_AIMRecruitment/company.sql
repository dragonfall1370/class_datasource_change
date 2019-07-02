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
cte_company AS (
	SELECT
	sc.idcompany company_id,
	TRIM(REPLACE(REPLACE(c.company_name,'LTd', 'Ltd'), 'Noble foods', 'Noble Foods')) company_name,
	ROW_NUMBER() OVER(PARTITION BY TRIM(REPLACE(REPLACE(c.company_name,'LTd', 'Ltd'), 'Noble foods', 'Noble Foods')) ORDER BY sc.idcompany) rn,
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
	cs.value state,
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
	END country,
	cx.address_default_post_code postal_code,
	CASE 
		WHEN strpos(cx.default_url, 'http') = 0 THEN concat('http://', cx.default_url) 
		ELSE cx.default_url
	END website,
	u.user_email company_owner,
-- 	d.new_document_name company_document,
	concat(
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_id::TEXT, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company id: ', REPLACE(cx.company_id::TEXT, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_reference, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company reference: ', REPLACE(cx.company_reference, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.preferred_supplier, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Preferred supplier: ', REPLACE(cx.preferred_supplier, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.alert_text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Alert text: ', REPLACE(cx.alert_text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.branch, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Branch: ', REPLACE(cx.branch, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN cx.is_top_of_group = 0 THEN concat('Is top of group: No', E'\n') ELSE concat('Is top of group: Yes', E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.last_attachment_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Last attachment date: ', REPLACE(cx.last_attachment_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.group_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Group name: ', REPLACE(cx.group_name, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.owning_user, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Owning user: ', REPLACE(cx.owning_user, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_phone_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company phone other: ', REPLACE(cx.company_phone_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.default_email, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default company email: ', REPLACE(cx.default_email, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_email1, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company email 1: ', REPLACE(cx.company_email1, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_email_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company email other: ', REPLACE(cx.company_email_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(coo.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company origin: ', REPLACE(coo.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cr.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company rating: ', REPLACE(cr.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(css.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company status: ', REPLACE(css.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(l.id_location, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company location: ', REPLACE(l.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(col.off_limit_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit by: ', REPLACE(col.off_limit_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(col.off_limit_note, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit note: ', REPLACE(col.off_limit_note, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(olt.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit type: ', REPLACE(olt.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cca.alias_reason, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Alias reason: ', REPLACE(cca.alias_reason, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cca.alias_note, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Alias note: ', REPLACE(cca.alias_note, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.client_relationship_notes, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Client relationship note: ', E'\n', REPLACE(cx.client_relationship_notes, '\x0d\x0a', ' '), E'\n', E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_note, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company note: ', E'\n', REPLACE(cx.company_note,  '\x0d\x0a', ' '), E'\n', E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cx.company_comment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company comment: ', E'\n', REPLACE(cx.company_comment, '\x0d\x0a', ' '), E'\n') END
	) note
	
	FROM selected_company sc
	LEFT JOIN company c ON sc.idcompany = c.id_company
	LEFT JOIN company_x cx ON c.id_company = cx.id_company
	LEFT JOIN cte_parent_company cpc ON cx.parent_id = cpc.id_company
	LEFT JOIN "user" u ON c.id_user = u.id_user
	LEFT JOIN country co ON cx.address_default_id_country_string = co.id_country
	LEFT JOIN county_state cs ON cx.address_default_county_state = cs.abbreviation AND cs.value <> 'Californian'
	LEFT JOIN company_origin coo ON cx.id_company_origin_string = coo.id_company_origin
	LEFT JOIN company_rating cr ON cx.id_company_rating_string = cr.id_company_rating
	LEFT JOIN company_status css ON cx.id_company_status_string = css.id_company_status
	LEFT JOIN "location" l ON cx.id_location_string = l.id_location
	LEFT JOIN company_off_limit col ON cx.id_company = col.id_company
	LEFT JOIN off_limit_type olt ON col.id_off_limit_type = olt.id_off_limit_type
	LEFT JOIN cte_company_alias cca ON cx.id_company = cca.id_company AND cca.rn = 1 
)

SELECT
company_id "company-externalId",
CASE
	WHEN rn <> 1 THEN concat(company_name, ' ', rn)
	ELSE company_name
END "company-name",
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
FROM cte_company