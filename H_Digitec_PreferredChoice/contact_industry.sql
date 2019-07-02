WITH cte_contacts as (
	SELECT
		con.field2 contact_id,
		com.field2 company_id,
		con.field7 contact_position,
		con.field8 department,
		con.field9 telephone,
		con.field38 mobile,
		con.field40,
		con.field27 notes
	FROM [Company Contact Database] con
	LEFT JOIN [Company Database] com on con.field1 = com.field2
),
company_industry AS (
	SELECT
	cd.Field2 company_id,
	vincere_industry_field,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(vincere_industry_field, ', ,', ','), 
	'retail', 'Retail'), 'fashion', 'Fashion'), 'charity', 'Charity'), 'utilities', 'Utilities'), 'digital', 'Digital'),'Ebusiness', 'eBusiness'),'marketing', 'Marketing'),
	'integrated', 'Integrated'), 'financial', 'Financial'), 'travel', 'Travel'), 'ebusiness', 'eBusiness'), 'hospitality', 'Hospitality'), 'insurance', 'Insurance'), 'advertising', 'Advertising'),
	'information', 'Information'), 'entertainment', 'Entertainment'), 'mobile', 'Mobile'), 'software', 'Software'), 'creative', 'Creative'), 'media', 'Media'), 'Fintech', 'FinTech'),
	'public', 'Public'), 'DO NOT CALL.', 'DO NOT CALL'), 'branding', 'Branding'), 'apps', 'Apps'), 'pharmaceutical', 'Pharmaceuticals'), 'Pharmaceuticalss', 'Pharmaceuticals'),
	'Eservices', 'eService'), 'Hospitality & Catering', 'Hospital and Catering'), 'E-business', 'eBusiness'), 'Educational', 'Education'), 'Digital Communcations', 'Digital Communications'),
	'Hospitality and Catering', 'Hospital and Catering'), 'Consulting', 'Consultancy'), 'Campaign', 'Campaigns'), 'Campaignss', 'Campaigns'), 'Email Marketing', 'eMail Marketing'),
	'Artifical Intelligence', 'Artificial Intelligence'), 'manufacturing', 'Manufacturing') industry
	FROM [Company Database] cd
	JOIN industry_area ai ON CONVERT(VARCHAR(MAX), cd.Field12) = ai.digitec_business_sector_field
),
split_industry AS (
	SELECT 
	company_id, 
	industry all_industry,
	TRIM(f.Item) industry,
	ROW_NUMBER() OVER(PARTITION BY company_id, TRIM(f.Item) ORDER BY company_id) rn
	FROM company_industry AS ci CROSS APPLY dbo.split_strings(ci.industry, ',') as f
)
SELECT
c.contact_id,
c.company_id,
CASE
	WHEN industry = 'Communications' THEN 'Digital Communications'
	WHEN industry = 'Digital Media' THEN 'Digital Communications'
	WHEN industry = 'Gambling / Games' THEN 'Gaming / Games'
	WHEN industry = 'Web' THEN 'Digital Communications'
	WHEN industry = 'Web Development' THEN 'Web Build'
	WHEN industry = 'Web Design' THEN 'Creative'
	ELSE industry
END industry,
CURRENT_TIMESTAMP as insert_timestamp
FROM cte_contacts c
JOIN split_industry i ON c.company_id = i.company_id AND i.rn = 1
AND industry <> ''