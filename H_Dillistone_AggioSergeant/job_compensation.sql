WITH cte_job AS (
	SELECT
	a.id_assignment job_id,
	ROW_NUMBER() OVER(PARTITION BY a.id_assignment ORDER BY ac.contacted_on ASC) rn,
	a.salary_from actual_salary,
	a.salary_to job_salary_to
	FROM assignment_contact ac
	JOIN "assignment" a ON ac.id_assignment = a.id_assignment AND a.is_deleted = 0
	WHERE a.is_deleted = 0
)

SELECT
job_id,
actual_salary salary_from,
job_salary_to
FROM cte_job j
WHERE rn = 1