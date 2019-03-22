WITH edu AS (
	SELECT
		c.contact_id candidate_id,
		c.education_school1 schoolName1,
		c.edu_degree_name1 degreeName1,
		'0' gpa1,
		'0' grade1,
		CASE
			WHEN c.edu_degree_type1 IN ('some high school or equivalent', 'high school or equivalent', 'secondary') THEN '12' --HIGH_SCHOOL_GRADUATE
			WHEN c.edu_degree_type1 IN ('vocational', 'certification') THEN '11' --NO_FORMAL_EDUCATION
			WHEN c.edu_degree_type1 IN ('some college', 'intermediategraduate', 'HND/HNC or equivalent') THEN '5' --DIPLOMA
			WHEN c.edu_degree_type1 IN ('doctorate') THEN '1' --DOCTORATE
			WHEN c.edu_degree_type1 IN ('associates', 'bachelors') THEN '4' --DEGREE
			WHEN c.edu_degree_type1 IN ('some post-graduate') THEN '3' --POST_GRAD_DIPLOMA
			WHEN c.edu_degree_type1 IN ('masters') THEN '2' --MASTER
			WHEN c.edu_degree_type1 IN ('professional') THEN '7' --PROFESSIONAL_QUALIFICATION
		END educationId1,
		
		c.education_school2 schoolName2,
		c.edu_degree_name2 degreeName2,
		'0' gpa2,
		'0' grade2,
		CASE
			WHEN c.edu_degree_type2 IN ('some high school or equivalent', 'high school or equivalent', 'secondary') THEN '12' --HIGH_SCHOOL_GRADUATE
			WHEN c.edu_degree_type2 IN ('vocational', 'certification') THEN '11' --NO_FORMAL_EDUCATION
			WHEN c.edu_degree_type2 IN ('some college', 'intermediategraduate', 'HND/HNC or equivalent') THEN '5' --DIPLOMA
			WHEN c.edu_degree_type2 IN ('doctorate') THEN '1' --DOCTORATE
			WHEN c.edu_degree_type2 IN ('associates', 'bachelors') THEN '4' --DEGREE
			WHEN c.edu_degree_type2 IN ('some post-graduate') THEN '3' --POST_GRAD_DIPLOMA
			WHEN c.edu_degree_type2 IN ('masters') THEN '2' --MASTER
			WHEN c.edu_degree_type2 IN ('professional') THEN '7' --PROFESSIONAL_QUALIFICATION
		END educationId2
	FROM contact c
	WHERE c.record_type_id = '01261000000gXaw'
)

SELECT
candidate_id,
concat(
CASE
	WHEN schoolName1 IS NOT NULL OR degreeName1 IS NOT NULL OR educationId1 IS NOT NULL THEN
	concat('[', json_build_object('schoolName', COALESCE(schoolName1, ''), 'degreeName', COALESCE(degreeName1, ''), 'gpa', gpa1, 'grade', grade1, 'educationId', educationId1), 
				CASE WHEN schoolName2 IS NOT NULL OR degreeName2 IS NOT NULL OR educationId2 IS NOT NULL THEN ',' END)
	WHEN (schoolName1 IS NULL OR degreeName1 IS NULL OR educationId1 IS NULL) AND (schoolName2 IS NOT NULL OR degreeName2 IS NOT NULL OR educationId2 IS NOT NULL) THEN '['
END,
CASE
	WHEN schoolName2 IS NOT NULL OR degreeName2 IS NOT NULL OR educationId2 IS NOT NULL THEN
	concat(json_build_object('schoolName', COALESCE(schoolName2, ''), 'degreeName', COALESCE(degreeName2, ''), 'gpa', gpa2, 'grade', grade2, 'educationId', educationId2), ']')
	WHEN (schoolName2 IS NULL OR degreeName2 IS NULL OR educationId2 IS NULL) AND (schoolName1 IS NOT NULL OR degreeName1 IS NOT NULL OR educationId1 IS NOT NULL) THEN ']'
END
) AS data

FROM edu