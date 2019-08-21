WITH main_company AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(name)) ORDER BY create_timestamp) rn
	FROM organisation
),
hotlist AS (
	SELECT
	object_ref  AS company_id,
	CASE
		WHEN savelist_ref = '21250	' THEN 'HIT LIST COMPANIES'
		WHEN savelist_ref = '112054' THEN 'TEMP TRAITOR 2016 CLIENTS'
		WHEN savelist_ref = '116673' THEN 'CHARLOTTE HIT LIST'
		WHEN savelist_ref = '116247' THEN 'EMMA HIT LIST'
		WHEN savelist_ref = '116687' THEN 'SARA HIT LIST -COMPANIES'
		WHEN savelist_ref = '114237' THEN 'BD List'
		WHEN savelist_ref = '112848' THEN 'KB=Traitor List (mixture of X Consultants)'
		WHEN savelist_ref = '113228' THEN 'KB (Prev HW list)'
		WHEN savelist_ref = '115176' THEN 'KB (Watford location clients)'
		WHEN savelist_ref = '111341' THEN 'Traitor List (Was AL...now KB)'
		WHEN savelist_ref = '116711' THEN 'A* KB list=Mar 19 = GOLD'
		WHEN savelist_ref = '112129' THEN 'Top 20'
		WHEN savelist_ref = '112035' THEN 'bronze'
		WHEN savelist_ref = '112583' THEN 'Silver clients'
		WHEN savelist_ref = '112582' THEN 'gold clients'
		WHEN savelist_ref = '112625' THEN 'silver(2)'
		WHEN savelist_ref = '113519' THEN 'Bronze'
		WHEN savelist_ref = '113520' THEN 'gold (2)'
		WHEN savelist_ref = '112022' THEN 'traitor'
	END group_name
	FROM savelist_entry
	WHERE savelist_ref IN ('21250','112054','3310','116673','116247','116687','114237','112848','113228','115176','111341','116711','112129','112035',
													'112583','112582','112625','113519','113520','112022')
),
company_hotlist AS (
SELECT
	c.organisation_ref AS company_id,
	CASE
		WHEN ch.group_name = 'bronze' THEN '1'
		WHEN ch.group_name = 'SARA HIT LIST -COMPANIES' THEN '2'
		WHEN ch.group_name = 'KB=Traitor List (mixture of X Consultants)' THEN '3'
		WHEN ch.group_name = 'Top 20' THEN '4'
		WHEN ch.group_name = 'Silver clients' THEN '5'
		WHEN ch.group_name = 'BD List' THEN '6'
		WHEN ch.group_name = 'KB (Prev HW list)' THEN '7'
		WHEN ch.group_name = 'gold (2)' THEN '8'
		WHEN ch.group_name = 'silver(2)' THEN '9'
		WHEN ch.group_name = 'A* KB list=Mar 19 = GOLD' THEN '10'
		WHEN ch.group_name = 'Bronze' THEN '11'
		WHEN ch.group_name = 'Traitor List (Was AL...now KB)' THEN '12'
		WHEN ch.group_name = 'CHARLOTTE HIT LIST' THEN '13'
		WHEN ch.group_name = 'gold clients' THEN '14'
		WHEN ch.group_name = 'EMMA HIT LIST' THEN '15'
		WHEN ch.group_name = 'traitor' THEN '16'
		WHEN ch.group_name = 'KB (Watford location clients)' THEN '17'
		WHEN ch.group_name = 'TEMP TRAITOR 2016 CLIENTS' THEN '18'
	END hotlist
FROM main_company c
JOIN hotlist ch ON c.organisation_ref = ch.company_id
WHERE group_name IS NOT NULL
)
SELECT
company_id,
string_agg(hotlist, ',') hotlist,
'add_com_info' additional_type,
1006 form_id,
11276 field_id,
'11276' constraint_id,
CURRENT_TIMESTAMP insert_timestamp
FROM company_hotlist
GROUP BY company_id, additional_type, form_id, field_id, constraint_id, insert_timestamp