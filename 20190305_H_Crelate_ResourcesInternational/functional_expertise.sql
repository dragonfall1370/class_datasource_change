WITH FE AS (
SELECT
tr.tag_relationship_id,
tr.target_entity_id,
t.name

FROM tag_relationships tr
LEFT JOIN tags t ON tr.right_entity_id = t.tag_id 
),
distinct_FE AS(
SELECT
DISTINCT name
FROM FE
),
lookup_FE AS (
	SELECT 
	ROW_NUMBER() OVER() fe_id,
	name
	FROM distinct_FE
),
-- contact_FE AS (
-- SELECT
-- c.contact_id,
-- t.name,
-- t.created_on::timestamp created_on
-- -- tag_category_id tag_category
-- FROM tags t
-- LEFT JOIN tag_relationships tr ON t.tag_id = tr.right_entity_id
-- LEFT JOIN contacts c ON tr.target_entity_id = c.contact_id
-- WHERE tr."TargetEntityId_Type" = 'Contacts'
-- AND c.record_type IN ('Sales/Client Contact', 'Candidate, Sales/Client Contact')
-- )

-- candidate_FE AS (
-- SELECT
-- c.contact_id candidate_id,
-- t.name,
-- t.created_on::timestamp created_on
-- -- tag_category_id tag_category
-- FROM tags t
-- LEFT JOIN tag_relationships tr ON t.tag_id = tr.right_entity_id
-- LEFT JOIN contacts c ON tr.target_entity_id = c.contact_id
-- WHERE tr."TargetEntityId_Type" = 'Contacts'
-- AND c.record_type IN ('Candidate', 'Candidate, Sales/Client Contact')
-- )

job_FE AS (
SELECT
j.job_id,
t.name,
t.created_on::timestamp created_on
-- tag_category_id tag_category
FROM tags t
LEFT JOIN tag_relationships tr ON t.tag_id = tr.right_entity_id
LEFT JOIN jobs j ON tr.target_entity_id = j.job_id
WHERE tr."TargetEntityId_Type" = 'Jobs'
)

SELECT
jfe.*,
lfe.fe_id
FROM job_FE jfe
LEFT JOIN lookup_FE lfe ON jfe.name = lfe.name
