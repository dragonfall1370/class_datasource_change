WITH sub_functional_expertise AS (
SELECT 'Marketing' AS functional_expertise, 'Ad Operations' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'B2B marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Brand Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Campaign Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Category Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Chief Marketing Officer' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Communications & PR' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'CRM' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'CRO Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Data Analyst Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Digital Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'eCommerce' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Events Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Insight & Analytics' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Marketing Events' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Marketing Strategy' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Media Planning / Buying' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'PPC' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Product Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Research Marketing' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'SEO' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Marketing' AS functional_expertise, 'Social Media' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, '' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Business Analysts' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Chief Digital Officer' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Digital Project Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Events Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'IT Programme Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'IT Project Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'PMO Analyst' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'PMO Assurance' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Product Developer' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Product Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Project Administrator' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Project Director' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Project Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Project Resource Planner' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Project Support/Co-ordinator' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Risk Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Project Management' AS functional_expertise, 'Transformation and Change Manager' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Sales' AS functional_expertise, 'COO' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
UNION ALL
SELECT 'Sales' AS functional_expertise, 'MD / CEO / VP' AS sub_functional_expertise, CURRENT_TIMESTAMP AS insert_timestamp
)
SELECT *, ROW_NUMBER() OVER() AS id
FROM sub_functional_expertise
WHERE sub_functional_expertise <> ''