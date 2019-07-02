WITH candidate_skill AS (
	SELECT
		c.person_id candidate_id,
		GROUP_CONCAT(
			CONCAT_WS(
				', ',
				RTRIM(fq.qualification),
				RTRIM(co.skill),
				RTRIM(c.languageString)
			) SEPARATOR ', ') skill
		
	FROM alpha_candidate c
	LEFT JOIN alpha_sel_fin_qual fq ON c.finQual_id = fq.id
	LEFT JOIN (SELECT
							candidate_id,
							GROUP_CONCAT(RTRIM(name) SEPARATOR ', ') skill
						FROM alpha_competency 
						WHERE description LIKE '%Skill%'
						GROUP BY candidate_id) co ON c.person_id = co.candidate_id
	WHERE fq.qualification IS NOT NULL OR co.skill IS NOT NULL
	GROUP BY candidate_id
)

SELECT
	c.person_id candidate_id,
	cs.skill skills

FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
LEFT JOIN candidate_skill cs ON c.id = cs.candidate_id
