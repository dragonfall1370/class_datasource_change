With parent_company AS (
	SELECT 
	a.account_id,
	a.name company_name,
	p.name parent_name
	FROM account a
	LEFT JOIN
	(SELECT DISTINCT account_id, name FROM account) p ON a.parent_id = p.account_id
	WHERE p.name is not NULL
),
cte_company AS (
	SELECT
		a.account_id,
		name,
		pc.parent_name,
		ROW_NUMBER() OVER(PARTITION by name ORDER BY a.created_date) rn,
		a.created_date, --injection
		parent_id, -- to find out
		COALESCE(
			CASE 
				WHEN RIGHT(billing_street, 1) = ',' OR RIGHT(billing_street, 1) = '.' THEN LEFT(billing_street, CHAR_LENGTH(billing_street) - 1)
				ELSE billing_street
			END,
			billing_city,
			billing_state,
			billing_country
		) location_name,
		billing_city city,
		billing_state state,
		billing_postal_code postal_code,
		CASE
			WHEN billing_country = 'South Africa' THEN 'ZA'
		END country,
		concat(
			CASE WHEN billing_street IS NOT NULL THEN CASE 
																									WHEN RIGHT(billing_street, 1) = ',' OR RIGHT(billing_street, 1) = '.' THEN LEFT(billing_street, CHAR_LENGTH(billing_street) - 1)
																									ELSE billing_street
																								END
			END,
			CASE WHEN billing_street IS NOT NULL AND billing_city IS NOT NULL THEN concat(', ', billing_city) ELSE billing_city END,
			CASE WHEN billing_street IS NOT NULL AND billing_city IS NOT NULL AND billing_state IS NOT NULL THEN concat(', ', billing_state) ELSE billing_state END,
			CASE WHEN billing_street IS NOT NULL AND billing_city IS NOT NULL AND billing_state IS NOT NULL AND billing_country IS NOT NULL THEN concat(', ', billing_country) ELSE billing_country END
		) address,
		COALESCE(a.phone) switch_board,
		COALESCE(fax, '') fax,
		COALESCE(
		CASE 
			WHEN strpos(website, 'http') = 0 THEN concat('http://', website) 
			ELSE website
		END, '') website,
		u.email,
		industry, --injection
		concat(
		CASE WHEN custom_picklist1 IS NULL THEN '' ELSE concat('Custom pick list: ', custom_picklist1, E'\n') END,
		CASE WHEN description IS NULL THEN '' ELSE concat('Description: ', E'\n', description) END
		) note
	FROM account a
	LEFT JOIN "user" u ON a.owner_id = u.user_id
	LEFT JOIN parent_company pc ON a.account_id = pc.account_id
	WHERE a.is_deleted = 0
)
SELECT
account_id "company-externalId",
CASE
	WHEN rn <> 1 THEN concat(name, ' ', rn)
	ELSE name
END "company-name",
parent_name "company-headQuarter",
location_name "company-locationName",
city "company-locationCity",
state "company-locationState",
country "company-locationCountry",
address "company-locationAddress",
postal_code "company-locationZipCode",
switch_board "company-switchBoard",
fax "company-fax",
website "company-website",
email "company-owners",
note "company-note"

from cte_company
