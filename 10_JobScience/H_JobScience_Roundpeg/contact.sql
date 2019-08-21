WITH cte_contact AS (
	SELECT
		c.contact_id,
		CASE
			WHEN a.account_id IS NULL THEN '1'
			ELSE a.account_id
		END account_id,
		c.first_name,
		c.middle_name,
		c.last_name,
		c.salutation,
		c.birthdate,
		c.phone,
		c.mobile_phone,
		c.email,
		ROW_NUMBER() OVER(PARTITION BY c.email ORDER BY c.contact_id) rn_email,
		c.title,
		u.email owner_email,
-- 		att.name contact_document,
-- 		ROW_NUMBER() OVER(PARTITION BY c.contact_id ORDER BY CASE WHEN att.name like '%.doc%' THEN 1
-- 																															WHEN att.name LIKE '%.pdf' THEN 2
-- 																															ELSE 3
-- 																													END, a.created_date DESC) rn_doc,
		concat(
			CASE WHEN mailing_street IS NOT NULL OR mailing_city IS NOT NULL OR mailing_state IS NOT NULL OR mailing_country IS NOT NULL THEN
			concat(
				'Contact address: ',
				CASE WHEN mailing_street IS NOT NULL THEN CASE 
																										WHEN RIGHT(mailing_street, 1) = ',' OR RIGHT(mailing_street, 1) = '.' THEN LEFT(mailing_street, CHAR_LENGTH(mailing_street) - 1)
																										ELSE REPLACE(mailing_street, ' .', '')
																									END
				END,
				CASE WHEN mailing_street IS NOT NULL AND mailing_city IS NOT NULL THEN concat(', ', mailing_city) ELSE mailing_city END,
				CASE WHEN mailing_street IS NOT NULL AND mailing_city IS NOT NULL AND mailing_state IS NOT NULL THEN concat(', ', mailing_state) ELSE mailing_state END,
				CASE WHEN mailing_street IS NOT NULL AND mailing_city IS NOT NULL AND mailing_state IS NOT NULL AND mailing_country IS NOT NULL THEN concat(', ', mailing_country) ELSE mailing_country END,
				E'\n'
			) END,
			CASE WHEN mailing_postal_code IS NULL THEN '' ELSE concat('Postal code: ', mailing_postal_code, E'\n') END,
			CASE WHEN best_fit IS NULL THEN '' ELSE concat('Best fit: ', best_fit, E'\n') END,
			CASE WHEN employer_org_name1 IS NULL THEN '' ELSE concat('Employer organization name: ', employer_org_name1) END
		) note
	FROM contact c
	LEFT JOIN account a ON c.account_id = a.account_id
	LEFT JOIN "user" u ON c.owner_id = u.user_id
-- 	LEFT JOIN attachment att ON c.contact_id = att.parent_id
	WHERE c.record_type_id = '01261000000gXax'
	AND c.is_deleted = 0
)

SELECT
contact_id "contact-externalId",
account_id "contact-companyId",
first_name "contact-firstName",
middle_name "contact-middleName",
last_name "contact-lastName",
salutation "contact-title",
birthdate "contact-dob",
phone "contact-phone",
mobile_phone "contact-mobilePhone",
email "contact-email",
title "contact-jobTitle",
owner_email "contact-owners",
-- contact_document,
note "contact-Note"
FROM cte_contact c
-- LEFT JOIN attachment a ON c.contact_id = a.parent_id
-- where rn_doc = 1