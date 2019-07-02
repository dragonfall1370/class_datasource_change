WITH contact_record_status AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		CASE
			WHEN email_address NOT LIKE '%@%' THEN NULL
			WHEN STRPOS(email_address, '''') = 1 AND RIGHT(email_address, 1) = '''' THEN LEFT(RIGHT(email_address, LENGTH(email_address) - 1), LENGTH(RIGHT(email_address, LENGTH(email_address) - 1)) - 1)
			ELSE email_address
		END AS contact_email,
		p.*
	FROM position p
	LEFT JOIN lookup l ON p.record_status = l.code
	WHERE code_type = '132'
	AND NULLIF(person_ref, '0') IS NOT NULL
),
current_contact AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY TRIM(LOWER(contact_email)) ORDER BY CASE WHEN contact_status = '1' THEN 1 ELSE 2 END) rn_email
	FROM contact_record_status
	WHERE rn = 1
),
contact_distribution_list AS (
	SELECT 
		object_ref AS  contact_id,
		CASE
			WHEN savelist_ref IN ('115814', '111384', '112854') THEN 'thuntley@hamlinknight.co.uk'
			WHEN savelist_ref IN ('113916', '116598', '104120') THEN 'pberry@hamlinknight.co.uk'
			WHEN savelist_ref IN ('116736') THEN 'charlotte.clarke@hamlinknight.co.uk'
			WHEN savelist_ref IN ('115502', '116676', '115540') THEN 'sam.waites@hamlinknight.co.uk'
			WHEN savelist_ref IN ('116253') THEN 'Emma.Herron@hamlinknight.co.uk'
			WHEN savelist_ref IN ('116688') THEN 'sara.homer@hamlinknight.co.uk'
			WHEN savelist_ref IN ('114125', '115874', '114210', '116469', '116729') THEN 'cbarnes@hamlinknight.co.uk'
			WHEN savelist_ref IN ('112842') THEN 'suky.rahim@hamlinknight.co.uk'
		END AS consultant,
		CASE
			WHEN savelist_ref = '115814'	THEN 'IMMEDIATELY AVAILABLES'
			WHEN savelist_ref = '111384'	THEN 'HIT LIST CONTACTS'
			WHEN savelist_ref = '112854'	THEN 'PERM CANDIDATES -TAMS'
			WHEN savelist_ref = '113916'	THEN 'PERM CANDIDATES PB'
			WHEN savelist_ref = '116598'	THEN 'BEZZA TEMPS'
			WHEN savelist_ref = '104120'	THEN 'CONTACT LIST PAUL HIT LIST CONTACTS 2015'
			WHEN savelist_ref = '116736'	THEN 'Charlotte Contact List'
			WHEN savelist_ref = '115502'	THEN 'SAM HIT LIST'
			WHEN savelist_ref = '116676'	THEN 'leam/war/ken mailer'
			WHEN savelist_ref = '115540'	THEN 'mailer to HR & OPS'
			WHEN savelist_ref = '116253'	THEN 'Contact Sales List'
			WHEN savelist_ref = '116688'	THEN 'Sara 2019 Hot List'
			WHEN savelist_ref = '114125'	THEN 'Available Temps'
			WHEN savelist_ref = '115874'	THEN 'Nat''s Candidates'
			WHEN savelist_ref = '114210'	THEN 'Available Perm'
			WHEN savelist_ref = '116469'	THEN 'Person Finder (Quick search results)'
			WHEN savelist_ref = '116729'	THEN 'NS Candidates'
			WHEN savelist_ref = '112842'	THEN 'temp log candidates'
		END AS group_name
	FROM savelist_entry
	WHERE savelist_ref IN ('115814', '111384', '112854', '113916', '116598', '104120', '116736', '115502', '116676', '115540', '116253', '116688', '114125', '115874', '114210', '116469', '116729', '112842')
)

------------------------------------------------- Main query --------------------------------------------------------------------------
SELECT
cc.person_ref contact_id,
cdl.consultant,
cdl.group_name,
CURRENT_TIMESTAMP insert_timestamp
FROM current_contact cc
JOIN person p ON cc.person_ref = p.person_ref
JOIN contact_distribution_list cdl ON cc.person_ref = cdl.contact_id
ORDER BY group_name