WITH default_contact_company AS (
	SELECT 
		person_id contact_id,
		company_id,
		ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY created ASC) rn
	FROM alpha_position
),
cte_job AS (
	SELECT
		jo.id job_id,
		COALESCE(p.person_id, cc.contact_id) contact_id,
		ROW_NUMBER() OVER(PARTITION BY COALESCE(p.person_id, cc.contact_id) ORDER BY jo.id) rn_contact,
		jo.company_id,
		jo.roleTitle job_title,
		ROW_NUMBER() OVER(PARTITION BY p.person_id, jo.company_id, jo.roleTitle ORDER BY jo.id) rn_job,
		jo.jobOn open_date,
		jo.numPositions head_count,
		jo.salaryCurrency currency,
		CASE
			WHEN partTime = 1 THEN 'PART_TIME'
			ELSE 'FULL_TIME'
		END employment_type,
		CASE
			WHEN permanent = 1 THEN 'PERMANENT'
			WHEN temporary = 1 THEN 'TEMPORARY'
			ELSE 'CONTRACT'
		END job_type,
		CAST(contractLength AS UNSIGNED) contract_length,
		CONCAT_WS(
			CHAR(10),
			CASE WHEN shortSumm IS NOT NULL THEN CONCAT_WS(CHAR(10), '------------------','Short summary', '------------------', shortSumm, CHAR(10)) END,
			CASE WHEN respText IS NOT NULL THEN CONCAT_WS(CHAR(10), '------------------------','Responsibilities', '------------------------', respText, CHAR(10)) END,
			CASE WHEN skillsText IS NOT NULL THEN CONCAT_WS(CHAR(10), '----------', 'Skills', '----------', skillsText) END
		) public_description,
		CONCAT_WS(
			CHAR(10),
			CASE WHEN sjs.jobStatus IS NOT NULL THEN CONCAT('Job status: ', sjs.jobStatus) END,
			CASE WHEN sjf.jobFunction IS NOT NULL THEN CONCAT('Job function: ', sjf.jobFunction) END,
			CASE WHEN js.jobSource IS NOT NULL THEN CONCAT('Job source: ', js.jobSource) END,
			CASE WHEN jc.jobControl IS NOT NULL THEN CONCAT('Job control: ', jc.jobControl) END,
			CASE WHEN feePercent IS NOT NULL THEN CONCAT('Fee percent: ', feePercent) END,
			CASE WHEN feeAmount IS NOT NULL THEN CONCAT('Fee amount: ', feeAmount) END,
			CASE WHEN retained IS NOT NULL THEN CONCAT('Retained: ', retained) END,
			CASE WHEN forecastFee IS NOT NULL THEN CONCAT('Forecast fee: ', forecastFee) END,
			CASE WHEN salaryRange IS NOT NULL THEN CONCAT('Salary range: ', salaryRange) END,
			CASE WHEN advertiseSalary = 0 THEN 'Advertise salary: No' ELSE 'Advertise salary: Yes' END,
			CASE WHEN stretchSalary IS NOT NULL THEN CONCAT('Stretch salary: ', stretchSalary) END,
			CASE WHEN jo.salaryPeriod IS NOT NULL THEN CONCAT('Salary period: ', jo.salaryPeriod) END,
			CASE WHEN variableSalAmount IS NOT NULL THEN CONCAT('Variable salary amount: ', variableSalAmount) END,
			CASE WHEN variableSalPercent IS NOT NULL THEN CONCAT('Variable salary percent: ', variableSalPercent) END,
			CASE WHEN advertReady = 0 THEN 'Advert ready: No' ELSE 'Advert ready: Yes' END,
			CASE WHEN publish = 0 THEN 'Publish: No' ELSE 'Publish: Yes' END,
			CASE WHEN jo.benefits IS NOT NULL THEN CONCAT('Benefits: ', jo.benefits) END
		) note
	FROM alpha_job_opening jo
	JOIN alpha_company ac ON jo.company_id = ac.id
	LEFT JOIN alpha_position p ON jo.position_id = p.id
	LEFT JOIN alpha_sel_job_status sjs ON jo.jobStatus_id = sjs.id
	LEFT JOIN alpha_sel_job_function sjf ON jo.jobFunction_id = sjf.id
	LEFT JOIN alpha_sel_job_source js ON jo.jobSource_id = js.id
	LEFT JOIN alpha_sel_job_control jc ON jo.jobControl_id = jc.id
	LEFT JOIN default_contact_company cc ON jo.company_id = cc.company_id AND cc.rn = 1
)

SELECT
	job_id "position-externalId",
	COALESCE(contact_id, rn_contact + 1000000) "position-contactId",
-- 	company_id,
	CASE
		WHEN rn_job = 1 THEN job_title
		ELSE CONCAT_WS(' ', job_title, rn_job)
	END "position-title",
	open_date "position-startDate",
	head_count "position-headcount",
	currency "position-currency",
	employment_type "position-employmentType",
	job_type "position-type",
	contract_length "position-contractLength",
	public_description "position-publicDescription",
	note "position-comment"
FROM cte_job