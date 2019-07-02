WITH company as(
SELECT 
	a.account_id company_id,
	COALESCE(name, 'No company name') company_name,
	a.created_on::TIMESTAMP created_on,
	ROW_NUMBER() OVER(PARTITION BY a.account_id ORDER BY case when ad.category_type_id = 'Headquarters' then 1 else 2 end) rn,
	
	ad.category_type_id,
	COALESCE(ad.line1, ad.city, ad.state, '') location_name,
	concat(
		CASE WHEN ad.line1 IS NULL THEN '' ELSE concat(ad.line1,', ') END,
		CASE WHEN ad.city IS NULL THEN '' ELSE concat(ad.city, ',') END,
		CASE WHEN ad.state IS NULL THEN '' ELSE concat(ad.state) END
	) address,
	COALESCE(ad.city, '') city,
	COALESCE(ad.state, '') state,
	pn.value phone,
	e.value email,
-- -- Remove everything from brief except description as customer's requirement
-- 	concat(
-- 		CASE
-- 			WHEN created_by_id IS NULL THEN '' ELSE concat('User created by email: ', u.email)
-- 		END,
-- 		CASE 
-- 			WHEN status_id IS NULL THEN '' ELSE concat(E'\n', 'Status: ', status_id)
-- 		END,
-- 		CASE 
-- 			WHEN record_type IS NULL THEN '' ELSE concat(E'\n', 'Record type: ', record_type) 
-- 		END,
-- 		CASE 
-- 			WHEN strip_tags(description) IS NULL THEN '' ELSE concat(E'\n', 'Description: ', strip_tags(description))
-- 		END
-- 	) note
	COALESCE(strip_tags(description), '') note
FROM accounts a
LEFT JOIN users u ON a.created_by_id = u.user_id
LEFT JOIN addresses ad ON a.account_id = ad.target_entity_id
LEFT JOIN phone_numbers pn ON a.account_id = pn.target_entity_id AND pn.is_primary = 't'
LEFT JOIN email_addresses e ON a.account_id = e.target_entity_id
)

SELECT
company_id "company-externalId",
company_name "company-name",
created_on,
location_name "company-locationName",
address "company-locationAddress",
city "company-locationCity",
state "company-locationState",
rn,
phone "company-phone",
-- email "company-owners",
note "company-note"
FROM company
where rn = 1
-- WHERE rn = 2  -- for companies with multiple address, will be injected by pentaho