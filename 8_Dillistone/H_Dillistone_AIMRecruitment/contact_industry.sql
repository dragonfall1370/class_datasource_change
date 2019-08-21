WITH cte_industry AS (
	SELECT 
	id_person person_id, 
	TRIM(s.industry) industry
	FROM person_x px, UNNEST(string_to_array(px.industry_value_string, ',')) s(industry)
),
map_industry AS (
	SELECT
		person_id,
		CASE
			WHEN industry = 'AMBIENT-' THEN 'IND - Ambient'
			WHEN industry = 'AMBIENT-BAKERY' THEN 'IND - Ambient - Bakery'
			WHEN industry = 'AMBIENT-BAKERY-BISCUITS' THEN 'IND - Ambient - Bakery - Biscuits'
			WHEN industry = 'AMBIENT-BAKERY-BREAD' THEN 'IND - Ambient - Bakery - Bread'
			WHEN industry = 'AMBIENT-BAKERY-CAKES' THEN 'IND - Ambient - Bakery - Cakes'
			WHEN industry = 'AMBIENT-BREWING' THEN 'IND - Ambient - Brewing'
			WHEN industry = 'AMBIENT-CANNED AND BOTTLED' THEN 'IND - Ambient - Canned & Bottled'
			WHEN industry = 'AMBIENT-CEREALS' THEN 'IND - Ambient - Cereals'
			WHEN industry = 'AMBIENT-COFEE AND TEA' THEN 'IND - Ambient - Coffee & Tea'
			WHEN industry = 'AMBIENT-CONFECTIONERY' THEN 'IND - Ambient - Confectionery'
			WHEN industry = 'AMBIENT-DESSERTS' THEN 'IND - Ambient - Desserts'
			WHEN industry = 'AMBIENT-DISTRIBUTION' THEN 'IND - Ambient - Distribution'
			WHEN industry = 'AMBIENT-EDIBLE OILS' THEN 'IND - Ambient - Edible Oils'
			WHEN industry = 'AMBIENT-FREE FROM' THEN 'IND - Ambient - Free From'
			WHEN industry = 'AMBIENT-FRUIT AND VEG' THEN 'IND - Ambient - Fruit & Veg'
			WHEN industry = 'AMBIENT-INGREDIENTS' THEN 'IND - Ambient - Ingredients'
			WHEN industry = 'AMBIENT-PASTA' THEN 'IND - Ambient - Pasta'
			WHEN industry = 'AMBIENT-PET PRODUCTS' THEN 'IND - Ambient - Pet Products'
			WHEN industry = 'AMBIENT-POWDERS' THEN 'IND - Ambient - Powders'
			WHEN industry = 'AMBIENT-PULSES' THEN 'IND - Ambient - Pulses'
			WHEN industry = 'AMBIENT-READY MEALS' THEN 'IND - Ambient - Ready Meals'
			WHEN industry = 'AMBIENT-SNACKS' THEN 'IND - Ambient - Snacks'
			WHEN industry = 'AMBIENT-SPECIALITY PRODUCTS' THEN 'IND - Ambient - Speciality Products'
			WHEN industry = 'AMBIENT-SPREADS AND  SAUCES' THEN 'IND - Ambient - Spreads & Sauces'
			WHEN industry = 'CHILLED-' THEN 'IND - Chilled'
			WHEN industry = 'CHILLED-DAIRY' THEN 'IND - Chilled - Dairy'
			WHEN industry = 'CHILLED-DAIRY-CHEESE' THEN 'IND - Chilled - Dairy - Cheese'
			WHEN industry = 'CHILLED-DAIRY-EGGS' THEN 'IND - Chilled - Dairy - Eggs'
			WHEN industry = 'CHILLED-DAIRY-MILK' THEN 'IND - Chilled - Dairy - Milk'
			WHEN industry = 'CHILLED-DAIRY-SPREADS' THEN 'IND - Chilled - Dairy - Spreads'
			WHEN industry = 'CHILLED-DAIRY-YOGURT' THEN 'IND - Chilled - Dairy - Yogurt'
			WHEN industry = 'CHILLED-DESSERTS' THEN 'IND - Chilled - Desserts'
			WHEN industry = 'CHILLED-DISTRIBUTION' THEN 'IND - Chilled - Distribution'
			WHEN industry = 'CHILLED-DRINKS' THEN 'IND - Chilled - Drinks'
			WHEN industry = 'CHILLED-FISH' THEN 'IND - Chilled - Fish'
			WHEN industry = 'CHILLED-FREE FROM' THEN 'IND - Chilled - Free From'
			WHEN industry = 'CHILLED-FRUIT AND VEG' THEN 'IND - Chilled - Fruit & Veg'
			WHEN industry = 'CHILLED-MEAT' THEN 'IND - Chilled - Meat'
			WHEN industry = 'CHILLED-READY MEALS' THEN 'IND - Chilled - Ready Meals'
			WHEN industry = 'CHILLED-SANDWICHES' THEN 'IND - Chilled - Sandwiches'
			WHEN industry = 'CHILLED-SAVOURY PASTRY' THEN 'IND - Chilled - Savoury Pastry'
			WHEN industry = 'CHILLED-SOUPS and SAUCES' THEN 'IND - Chilled - Soups & Sauces'
			WHEN industry = 'CHILLED-SPECIALITY PRODUCTS' THEN 'IND - Chilled - Speciality Products'
			WHEN industry = 'CHILLED-SPREADS AND SAUCES' THEN 'IND - Chilled - Spreads & Sauces'
			WHEN industry = 'FROZEN-' THEN 'IND - Frozen '
			WHEN industry = 'FROZEN-BAKERY' THEN 'IND - Frozen - Bakery'
			WHEN industry = 'FROZEN-DAIRY' THEN 'IND - Frozen - Dairy'
			WHEN industry = 'FROZEN-DESSERTS' THEN 'IND - Frozen - Desserts'
			WHEN industry = 'FROZEN-DISTRIBUTION' THEN 'IND - Frozen - Distribution'
			WHEN industry = 'FROZEN-FISH' THEN 'IND - Frozen - Fish'
			WHEN industry = 'FROZEN-FREE FROM' THEN 'IND - Frozen - Free From'
			WHEN industry = 'FROZEN-FRUIT AND VEG' THEN 'IND - Frozen - Fruit & Veg'
			WHEN industry = 'FROZEN-MEAT' THEN 'IND - Frozen - Meat'
			WHEN industry = 'FROZEN-PASTRY' THEN 'IND - Frozen - Pastry'
			WHEN industry = 'FROZEN-READY MEALS' THEN 'IND - Frozen - Ready Meals'
			WHEN industry = 'FROZEN-SNACKS' THEN 'IND - Frozen - Snacks'
			WHEN industry = 'FROZEN-SPECIALITY PRODUCTS' THEN 'IND - Frozen - Speciality Products'
			WHEN industry = 'INTERIM MANAGEMENT' THEN 'IND - Interim Management'
			WHEN industry = 'NONFOOD-' THEN 'IND - Nonfood'
			WHEN industry = 'NONFOOD-AUTOMOTIVE' THEN 'IND - Nonfood - Automotive'
			WHEN industry = 'NONFOOD-ENGINEERING' THEN 'IND - Nonfood - Engineering'
			WHEN industry = 'NONFOOD-FACILITIES' THEN 'IND - Nonfood - Facilities'
			WHEN industry = 'NONFOOD-LOGISTICS' THEN 'IND - Nonfood - Logistics'
			WHEN industry = 'NONFOOD-MILITARY' THEN 'IND - Nonfood - Military'
			WHEN industry = 'NONFOOD-OTHER' THEN 'IND - Nonfood - Other'
			WHEN industry = 'NONFOOD-PACKAGING' THEN 'IND - Nonfood - Packaging'
			WHEN industry = 'NONFOOD-PHARMACEUTICALS' THEN 'IND - Nonfood - Pharma'
			WHEN industry = 'ALDI' THEN 'RET - Aldi'
			WHEN industry = 'ASDA' THEN 'RET - Asda'
			WHEN industry = 'BRANDED' THEN 'RET - Branded'
			WHEN industry = 'CO-OP' THEN 'RET - Co-Op'
			WHEN industry = 'FOODSERVICE' THEN 'RET - Foodservice'
			WHEN industry = 'LIDL' THEN 'RET - Lidl'
			WHEN industry = 'M&S' THEN 'RET - M&S'
			WHEN industry = 'MORRISONS' THEN 'RET - Morrisons'
			WHEN industry = 'OWN LABEL' THEN 'RET - Own Label'
			WHEN industry = 'RETAILER' THEN 'RET - Retailer'
			WHEN industry = 'SAINSBURYS' THEN 'RET - Sainsburys'
			WHEN industry = 'TESCO' THEN 'RET - Tesco'
			WHEN industry = 'WAITROSE' THEN 'RET - Waitrose'
			WHEN industry = 'SITE CLOSED' THEN 'Site Closed'
			WHEN industry = 'USE INTERIMS' THEN 'Use Interims'
			WHEN industry = 'UMBRELLA Co' THEN 'Umbrella Co'
		END industry
	FROM cte_industry
),
cte_contact AS (
	SELECT
	cp.id_person contact_id,
	ROW_NUMBER() OVER(PARTITION BY cp.id_person ORDER BY cp.sort_order ASC, cp.employment_from DESC, cp.is_default_role) rn
	FROM company_person cp
	LEFT JOIN selected_company sc ON cp.id_company = sc.idcompany
	JOIN person_x px ON cp.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON cp.id_person = p.id_person AND p.is_deleted = 0
),
distinct_contact_industry AS (
SELECT
person_id contact_id,
industry,
CURRENT_TIMESTAMP insert_timestamp,
ROW_NUMBER() OVER(PARTITION BY person_id, industry ORDER BY person_id) rn
FROM map_industry mi
JOIN cte_contact cc ON mi.person_id = cc.contact_id AND cc.rn = 1
WHERE industry IS NOT NULL
)
SELECT *
FROM distinct_contact_industry
WHERE rn = 1