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
),
candidate_skill AS (
	SELECT
		c.person_id candidate_id,
		GROUP_CONCAT(
			CONCAT_WS(
				', ',
				RTRIM(fq.qualification),
				RTRIM(co.skill)
			) SEPARATOR ', ') skill
-- 		c.languageString
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
),
candidate_lists AS (
	SELECT
		candidate_id,
		GROUP_CONCAT(listTitle SEPARATOR ', ') list
	FROM alpha_lists_candidates lc
	JOIN alpha_lists l ON lc.lists_id = l.id
	GROUP BY candidate_id
),
candidate_nationality AS (
	SELECT
		candidate_id,
		c.a2 nationality_code,
		ROW_NUMBER() OVER(PARTITION BY candidate_id ORDER BY n.created DESC) rn
	FROM alpha_nationality n
	JOIN alpha_sel_country c ON n.country_id = c.id
),
candidate_second_nationality AS (
	SELECT *
	FROM candidate_nationality
	WHERE rn = 2
)

SELECT
	c.person_id "candidate-externalId",
-- 	ROW_NUMBER() OVER(PARTITION BY c.person_id, p.lastName, c.email ORDER BY c.person_id) rn,
	p.firstName "candidate-firstName",
	p.lastName "candidate-lastName",
	CASE
		WHEN p.gender = 'Male' THEN 'MALE'
		WHEN p.gender = 'Female' THEN 'FEMALE'
	END "candidate-gender",
	DATE_FORMAT(p.dateOfBirth, '%Y-%m-%d') "candidate-dob",
	COALESCE(c.mobileNumber, c.secondMobileNumber) "candidate-mobile",
	COALESCE(c.mobileNumber, c.secondMobileNumber) "candidate-phone",
	c.homeNumber "candidate-homePhone",
	c.email "candidate-email",
	c.secondEmail "candidate-workEmail",
	c.linkedIn "candidate-linkedln",
	COALESCE(NULLIF(a.addressLine1, ''), NULLIF(a.addressLine2, ''), NULLIF(a.addressLine3, ''), NULLIF(a.city, ''), NULLIF(a.region, ''), NULLIF(a.townCity, '')) location_name,
	COALESCE(a.city, a.region, townCity) "candidate-city",
	a.county "candidate-State",
	COALESCE(
	REPLACE(REPLACE(REPLACE(REPLACE(a.countryCode, 'SCT', 'GB'), 'WLS', 'GB'), 'UK', 'GB'), 'NIR', 'IE'),
	REPLACE(a.countryUkCode, 'ENG', 'GB')
	) "candidate-Country",
	CONCAT_WS(
		', ',
		CASE WHEN NULLIF(a.addressLine1, '') IS NOT NULL THEN a.addressLine1 END,
		CASE WHEN NULLIF(a.addressLine2, '') IS NOT NULL THEN a.addressLine2 END,
		CASE WHEN NULLIF(a.addressLine3, '') IS NOT NULL THEN a.addressLine3 END,
		CASE WHEN NULLIF(a.city, '') AND NULLIF(a.region, '') IS NOT NULL THEN COALESCE(a.city, a.region, townCity) END,
		CASE WHEN NULLIF(a.county, '') IS NOT NULL THEN a.county END,
		CASE WHEN NULLIF(a.country, '') IS NOT NULL THEN a.country END
	) AS "candidate-address",
	latitude,
	longitude,
	a.postCode "candidate-zipCode",
	REPLACE(n.nationality_code, 'UK', 'GB') "candidate-citizenship",
	cs.skill "candidate-skills",
-- 	c.objective role_title,
	CASE
		WHEN c.partTime = 1 THEN 'PART_TIME'
		ELSE 'FULL_TIME'
	END "candidate-employmentType",
	CASE
		WHEN c.permanent = 1 THEN 'PERMANENT'
		WHEN c.temporary = 1 THEN 'TEMPORARY'
		ELSE 'CONTRACT'
	END "candidate-jobTypes",
	c.salaryAmount "candidate-currentSalary",
	c.salaryHiAmount "candidate-desiredSalary",
	ce.education_summary "candidate-education",
	CONCAT_WS(
		CHAR(10),
		CASE WHEN c.is_active = 1 THEN 'Active: Yes' ELSE 'Active: No' END,
		CASE WHEN cas.id IS NOT NULL THEN CONCAT('Candidate status: ', CONCAT_WS(' - ', cas.candStatus, cas.description)) END,
		CASE WHEN c.lastPlaced IS NOT NULL THEN CONCAT('Last placed date: ', c.lastPlaced) END,
		CASE WHEN c.last_placed_by IS NOT NULL THEN CONCAT('Last placed by: ', CONCAT_WS(' - ', u.username, u.email)) END,
		CASE WHEN cls.clientStatus IS NOT NULL THEN CONCAT('Client status: ', cls.clientStatus) END,
		CASE WHEN lic.list IS NOT NULL THEN CONCAT('Lists: ', lic.list) END,
		-- NI number [finding]
		CASE WHEN c.emailUnsubscribe = 1 THEN 'Email unsubscribe: Yes' ELSE 'Email unsubscribe: No' END,
		CASE WHEN c.smsUnsubscribe = 1 THEN 'SMS unsubscribe: Yes' ELSE 'SMS unsubscribe: No' END,
		CASE WHEN c.skype IS NOT NULL THEN CONCAT('Skype: ', c.skype) END,
		CASE WHEN sn.nationality_code IS NOT NULL THEN CONCAT('Second nationality: ', sn.nationality_code) END,
		CASE WHEN c.visaRequirements IS NOT NULL THEN CONCAT('Visa requirements: ', c.visaRequirements) END,
		CASE WHEN c.hobbies IS NOT NULL THEN CONCAT('Hobbies: ', c.hobbies) END,
		CASE WHEN c.created IS NOT NULL THEN CONCAT('Created: ', c.created) END,
		CASE WHEN c.updated IS NOT NULL THEN CONCAT('Updated: ', c.updated) END,
		CASE WHEN c.consultantNotes IS NOT NULL THEN CONCAT('Consultant notes: ', c.consultantNotes) END,
		CASE WHEN c.meetingNotes IS NOT NULL THEN CONCAT('Meeting notes: ', c.meetingNotes) END
		
-- 		Last Contacted:
-- 		Interviews:
	) "candidate-note"
	
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
LEFT JOIN alpha_address a ON c.address_id = a.id
LEFT JOIN alpha_sel_cand_status cas ON c.candStatus_id = cas.id
LEFT JOIN alpha_user u ON c.last_placed_by = u.id
LEFT JOIN alpha_sel_client_status cls ON c.clientStatus_id = cls.id
LEFT JOIN candidate_nationality n ON c.person_id = n.candidate_id AND n.rn = 1
LEFT JOIN candidate_education ce ON c.person_id = ce.candidate_id
LEFT JOIN candidate_skill cs ON c.person_id = cs.candidate_id
LEFT JOIN candidate_lists lic ON c.person_id = lic.candidate_id
LEFT JOIN candidate_second_nationality sn ON c.person_id = sn.candidate_id
WHERE c.person_id BETWEEN 200001 AND 300000