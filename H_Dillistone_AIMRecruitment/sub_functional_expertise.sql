WITH sub_functional_expertise AS (
SELECT 'Chief' AS functional_expertise, 'CEO' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Chief' AS functional_expertise, 'MD' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Chief' AS functional_expertise, 'Owner' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Commercial' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Commercial' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Commercial' AS functional_expertise, 'NAM/other' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Engineering' AS functional_expertise, 'Contractor' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Engineering' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Engineering' AS functional_expertise, 'Engineer' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Engineering' AS functional_expertise, 'Maintenance Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Engineering' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Engineering' AS functional_expertise, 'Project Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Engineering' AS functional_expertise, 'Projects' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Finance' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Finance' AS functional_expertise, 'Management Accountant' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Finance' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HR' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HR' AS functional_expertise, 'Lower' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HR' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HR' AS functional_expertise, 'Recruitment Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HR' AS functional_expertise, 'Resourcing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HR' AS functional_expertise, 'Business PArtner' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HR' AS functional_expertise, 'L&D Trainer' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HSE' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HSE' AS functional_expertise, 'Advisor' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'HSE' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Interim Manager' AS functional_expertise, '' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'International' AS functional_expertise, '' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Lean Manufacturing' AS functional_expertise, '' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Lower' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'NPD' AS functional_expertise, 'Dev Chef' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'NPD' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'NPD' AS functional_expertise, 'Innovation Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'NPD' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'NPD' AS functional_expertise, 'Project Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'NPD' AS functional_expertise, 'Technolgist' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'CI' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'General Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Low Up To 30K' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Mid 30 To 45K' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Mid 30 To 50K' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Mid 45 To 60K' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Senior 50 To 70K' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Senior 60 To 80K' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Top Level 70K Up' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'Top Level 80K Up' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Ops' AS functional_expertise, 'What Level' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Other' AS functional_expertise, 'Cost Reduction' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Other' AS functional_expertise, 'Franchise' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Other' AS functional_expertise, 'Industrial Eng' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Other' AS functional_expertise, 'Programme Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Other' AS functional_expertise, 'Trainer' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Other' AS functional_expertise, 'Unspecified' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Packaging' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Packaging' AS functional_expertise, 'Technologist' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Planning' AS functional_expertise, 'Analyst' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Planning' AS functional_expertise, 'Demand Planner' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Planning' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Planning' AS functional_expertise, 'Material Planner' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Planning' AS functional_expertise, 'Planner' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Planning' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Planning' AS functional_expertise, 'Production Planner' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Process' AS functional_expertise, 'Lower' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Process' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Process' AS functional_expertise, 'Tech' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Purchasing' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Purchasing' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Recruitment Consultant' AS functional_expertise, '' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Retired' AS functional_expertise, '' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Despatch Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Distrib Man' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Lower' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Materials Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Transport  Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Supply Chain' AS functional_expertise, 'Warehouse Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Auditor' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Compliance - Lower' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Compliance Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Group Hygiene Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Group Technical Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Group Technical Services Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Head Of Technical' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Hygiene Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Hygiene Supervisor' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Lab Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Lower' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Manager Low Level' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'QA Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'QA Supervisor' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Raw Materials Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Raw Materials Technologist' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Scientist' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Specifications Technologist' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Systems Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Technical' AS functional_expertise, 'Technical Services Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
)

SELECT *, ROW_NUMBER() OVER() AS id
FROM sub_functional_expertise
WHERE sub_functional_expertise <> ''