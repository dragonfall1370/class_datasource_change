WITH head_quarter AS (
	SELECT 
	id AS head_quarter_id,
	companyName AS head_quarter_name
	FROM alpha_company
	WHERE id IN (SELECT DISTINCT parentCompany_id AS parent_company_id FROM alpha_company)
),
selected_company AS (
	SELECT
		ct.company_id
	FROM companies_tags ct
	JOIN alpha_tag alt ON ct.tag_id = alt.id
	WHERE alt.id = 477
)

SELECT
c.id AS "company-externalId",
c.companyName AS "company-name",
c.phoneNumber AS "company-switchBoard",
hq.head_quarter_name AS "company-headQuarter",
CASE
	WHEN LENGTH(c.webSite) > 100 THEN SUBSTRING_INDEX(c.webSite, '/', 3)
	ELSE c.webSite
END "company-webSite",
CONCAT_WS(
	CHAR(10),
	CASE WHEN email IS NOT NULL THEN CONCAT('Email: ', email ) END,
	CASE WHEN twitter IS NOT NULL THEN CONCAT('Twitter: ', twitter) END,
	CASE WHEN linkedIn IS NOT NULL THEN CONCAT('LinkedIn: ', linkedIn) END,
	CASE WHEN jobsPage IS NOT NULL THEN CONCAT('Jobs Page: ', jobsPage) END,
	CASE WHEN emailFormat IS NOT NULL THEN CONCAT('Email Format: ', emailFormat) END,
	CASE WHEN marketSize IS NOT NULL THEN CONCAT('Market Size: ', marketSize) END,
	CASE WHEN turnover IS NOT NULL THEN CONCAT('Turnover: ', turnover) END,
	CASE WHEN employeesNum IS NOT NULL THEN CONCAT('Number of Employees: ', employeesNum) END,
	CASE WHEN companyRegNo IS NOT NULL THEN CONCAT('Company Registration No.: ', companyRegNo) END,
	CASE WHEN creditCheckResult IS NOT NULL THEN CONCAT('Credit Check Result: ', creditCheckResult) END,
	CASE WHEN creditCheckDate IS NOT NULL THEN CONCAT('Credit Check Date: ', creditCheckDate) END,
	CASE WHEN permPaymentTerms IS NOT NULL THEN CONCAT('Payment Terms - Perm: ', permPaymentTerms) END,
	CASE WHEN otherPaymentTerms IS NOT NULL THEN CONCAT('Payment Terms - Other: ', otherPaymentTerms) END,
	CASE WHEN termsAndConditions IS NOT NULL THEN CONCAT('Agreed Terms: ', termsAndConditions) END,
	CASE WHEN pSADate IS NOT NULL THEN CONCAT('PSA Date: ', pSADate) END,
	CASE WHEN s.clientStatus IS NOT NULL THEN CONCAT('Relationship: ', s.clientStatus) END,
	CASE WHEN aboutCompany IS NOT NULL THEN CONCAT('Company Bio: ', aboutCompany)END
) AS "company-note"

FROM alpha_company c
JOIN selected_company sc ON c.id = sc.company_id
LEFT JOIN head_quarter hq ON c.parentCompany_id = hq.head_quarter_id
LEFT JOIN alpha_sel_client_status s ON c.relationshipStatus_id = s.id
WHERE c.companyName IS NOT NULL
-- LIMIT 500