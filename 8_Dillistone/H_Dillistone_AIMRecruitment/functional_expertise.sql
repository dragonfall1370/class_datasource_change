WITH functional_expertise AS (
SELECT 'Chief' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Commercial' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Engineering' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Finance' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'HR' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'HSE' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Interim Manager' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'International' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Lean Manufacturing' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Marketing' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'NPD' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Ops' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Other' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Packaging' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Planning' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Process' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Purchasing' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Recruitment Consultant' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Retired' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
UNION ALL
SELECT 'Technical' AS functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp 
)
SELECT *, ROW_NUMBER() OVER() AS id
FROM functional_expertise