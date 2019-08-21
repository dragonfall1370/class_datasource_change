WITH main_company AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(name)) ORDER BY create_timestamp) rn
	FROM organisation
),
company_rating AS (
	SELECT organisation_ref,
	CASE
		WHEN l.description = 'Gold' THEN '1'
		WHEN l.description = 'Silver' THEN '2'
		WHEN l.description = 'Other' THEN '3'
		WHEN l.description = 'Mailer' THEN '4'
		WHEN l.description = 'Small business up to 50 emps' THEN '5'
		WHEN l.description = 'Medium business 50-100 emps' THEN '6'
		WHEN l.description = 'Large business 100+ emps' THEN '7'
		WHEN l.description = 'PSL' THEN '8'
		WHEN l.description = 'Public Sector' THEN '9'
		WHEN l.description = 'Competitor''s Client' THEN '10'
		WHEN l.description = 'Temp Traitor Client' THEN '11'
	END rating,
	ROW_NUMBER() OVER(PARTITION BY organisation_ref, l.description ORDER BY organisation_ref) rn
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE organisation_ref IS NOT NULL
	AND sc.code_type = '1040'
),
combine_rating AS (
	SELECT
		organisation_ref,
		string_agg(rating, ',') rating
	FROM company_rating
	WHERE rn = 1
	GROUP BY organisation_ref
)

------------------------------------------------- Main query --------------------------------------------------------------------------
SELECT
c.organisation_ref AS company_id,
cr.rating,
'add_com_info' additional_type,
1006 form_id,
11268 field_id,
'11268' constraint_id,
CURRENT_TIMESTAMP insert_timestamp
FROM main_company c
JOIN combine_rating cr ON c.organisation_ref = cr.organisation_ref