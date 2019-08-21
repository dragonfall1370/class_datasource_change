WITH selected_company AS (
	SELECT
		ct.company_id
	FROM companies_tags ct
	JOIN alpha_tag alt ON ct.tag_id = alt.id
	WHERE alt.id = 477
),
current_contact AS (
	SELECT
	cc.person_id contact_id,
	ROW_NUMBER() OVER(PARTITION BY cc.person_id ORDER BY STR_TO_DATE(REPLACE(cc.startDate, '/', '-'), '%d-%m-%Y') DESC) rn,
	STR_TO_DATE(REPLACE(cc.startDate, '/', '-'), '%d-%m-%Y') start_date,
	cc.company_id,
	ap.firstName first_name,
	ap.lastName last_name,
	cc.directNumber phone,
	cc.mobileNumber mobile,
	cc.email,
	cc.roleTitle job_title,
	CONCAT_WS(
		CHAR(10),
		CASE WHEN ap.gender IS NOT NULL THEN CONCAT('Gender: ', ap.gender) END,
		CASE WHEN srl.recruiterLevel IS NOT NULL THEN CONCAT('Recruiter level: ', srl.recruiterLevel) END,
		CASE WHEN sjf.jobFunction IS NOT NULL THEN CONCAT('Job function: ', sjf.jobFunction) END,
		CASE WHEN cc.startDate IS NOT NULL THEN CONCAT('[Current Role] Start date: ', cc.startDate) END,
		CASE WHEN ac.companyName IS NOT NULL THEN CONCAT('[Current Role] Office: ', ac.companyName) END,
		CASE WHEN cc.is_active = 0 THEN 'Active user: No' ELSE 'Active user: Yes' END,
		CASE WHEN tan.linkedInUrl IS NOT NULL THEN CONCAT('LinkedIn: ', tan.linkedInUrl) END,
		CASE WHEN cc.emailUnsubscribe = 0 THEN 'Email unsubscribe: No' ELSE 'Email unsubscribe: Yes' END,
		CASE WHEN ap.meetingNotes IS NOT NULL THEN CONCAT('Meeting notes: ', ap.meetingNotes) END
	) note
	FROM alpha_position cc
	JOIN alpha_person ap ON cc.person_id = ap.id
	JOIN alpha_company ac ON cc.company_id = ac.id
	JOIN selected_company sc ON cc.company_id = sc.company_id
	LEFT JOIN alpha_sel_recruiter_level srl ON cc.recruiterLevel_id = srl.id
	LEFT JOIN alpha_sel_job_function sjf ON cc.recruitsJobFunction_id = sjf.id
	LEFT JOIN table_appy_nitty tan ON cc.person_id = tan.person_id
	WHERE COALESCE(cc.directNumber, cc.email) IS NOT NULL
),
check_dup_email AS (
	SELECT *,
		CASE
			WHEN email IS NOT NULL THEN ROW_NUMBER() OVER(PARTITION BY email ORDER BY start_date DESC)
			ELSE 1
		END AS rn_email
	FROM current_contact
	WHERE rn = 1
)
SELECT
contact_id AS "contact-externalId",
company_id AS "contact-companyId",
first_name AS "contact-firstName",
last_name AS "contact-lastName",
phone "contact-phone",
mobile,
CASE
	WHEN rn_email = 2 THEN INSERT(email, 1, 0, 'DUP_')
	WHEN rn_email > 2 THEN INSERT(email, 1, 0, CONCAT('DUP_', rn_email, '_'))
	ELSE email
END AS "contact-email",
job_title AS "contact-positionTitle",
note AS "contact-note"
FROM check_dup_email