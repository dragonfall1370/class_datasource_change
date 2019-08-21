SELECT
a.id_assignment job_id,
string_agg(i.value, ',') industry,
CURRENT_TIMESTAMP insert_timestamp
FROM "assignment" a
JOIN assignment_code ac ON a.id_assignment = ac.id_assignment
JOIN industry i ON ac.code_id = i.id_industry
GROUP BY job_id