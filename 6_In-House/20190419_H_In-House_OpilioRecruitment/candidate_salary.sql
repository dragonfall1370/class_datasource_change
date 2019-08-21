WITH current_salary AS (
	SELECT
		person_id,
		salaryAmount current_salary,
		ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY startDate DESC) rn
	FROM alpha_position p
	WHERE salaryAmount IS NOT NULL
)
SELECT
c.person_id AS candidate_id,
current_salary,
c.salaryAmount AS desire_salary
FROM alpha_candidate c
LEFT JOIN current_salary cs ON c.person_id = cs.person_id
WHERE cs.rn = 1
AND COALESCE(current_salary, c.salaryAmount) IS NOT NULL