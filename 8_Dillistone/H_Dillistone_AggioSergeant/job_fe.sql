SELECT 
a.id_assignment job_id,
jf.value functional_expertise,
CURRENT_TIMESTAMP insert_timestamp
FROM "assignment" a
JOIN assignment_code ac ON a.id_assignment = ac.id_assignment
JOIN job_function jf ON ac.code_id = jf.id_job_function