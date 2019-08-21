WITH candidate_education AS (
	SELECT
		candidate_id,
		GROUP_CONCAT(
			CONCAT_WS(
				CHAR(10),
				CASE WHEN NULLIF(schoolName, '') IS NOT NULL THEN CONCAT('School name: ', schoolName) END,
				CASE WHEN NULLIF(organizationUnit, '') IS NOT NULL THEN CONCAT('Department: ', organizationUnit) END,
				CASE WHEN NULLIF(startDate, '') IS NOT NULL THEN CONCAT('Start date: ', startDate) END,
				CASE WHEN NULLIF(endDate, '') IS NOT NULL THEN CONCAT('End date: ', endDate) END,
				CASE WHEN NULLIF(degreeName, '') IS NOT NULL THEN CONCAT('Degree name: ', degreeName) END,
				CASE WHEN NULLIF(major, '') IS NOT NULL THEN CONCAT('Major: ', major) END,
				CASE WHEN NULLIF(measure, '') IS NOT NULL THEN CONCAT('Grade: ', measure) END,
				CASE WHEN NULLIF(degreeDate, '') IS NOT NULL THEN CONCAT('Degree date: ', degreeDate) END,
				CASE WHEN NULLIF(comments, '') IS NOT NULL THEN CONCAT('Comment: ', CHAR(10), comments) END
			) SEPARATOR '\n--------------------------------------------------------------------------------------------------\n'
		) education_summary
	FROM alpha_education
	GROUP BY candidate_id
)
SELECT
	CAST(c.person_id AS CHAR) candidate_id,
	ce.education_summary
	
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
LEFT JOIN candidate_education ce ON c.id = ce.candidate_id
