-- DROP TABLE IF EXISTS _03_job_sample;
-- CREATE TABLE _03_job_sample
-- AS
WITH current_contact AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY pos.person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		pos.*
	FROM position pos
	LEFT JOIN lookup l ON pos.record_status = l.code
	JOIN person p ON pos.person_ref = p.person_ref
	WHERE code_type = '132'
	ORDER BY pos.person_ref
),
contact_company AS (
	SELECT
		org.organisation_ref AS company_id,
		cc.person_ref AS contact_id,
		l.description,
		ROW_NUMBER() OVER(PARTITION BY org.organisation_ref ORDER BY l.code::int ASC) rn
	FROM organisation org
	LEFT JOIN current_contact cc ON cc.organisation_ref = org.organisation_ref
	LEFT JOIN lookup l ON cc.contact_status = l.code
	WHERE code_type = '135'
	AND cc.rn = 1
),
distinct_company AS (
	SELECT DISTINCT o.organisation_ref AS company_id
	FROM opportunity o
	LEFT JOIN contact_company cc ON o.organisation_ref = cc.company_id
	WHERE cc.contact_id IS NULL
),
distinct_default_contact AS (
	SELECT 
		company_id
	FROM distinct_company
	ORDER BY company_id::INT
),
generate_default_contact AS (
	SELECT
		company_id,
		ROW_NUMBER() OVER() AS contact_id
	FROM distinct_default_contact
),
opportunity_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '117'
),
job_status AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '119'
),
job_status_reason AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '265'
),
job_source AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '108'
),
job_analysis_category AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '608'
),
job_location AS (
	SELECT opportunity_ref,
	string_agg(l.description, ', ') AS locations
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE opportunity_ref IS NOT NULL
	AND sc.code_type = '1020'
	GROUP BY opportunity_ref
),
job_qualification AS (
	SELECT opportunity_ref,
	string_agg(l.description, ', ') AS qualifications
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE opportunity_ref IS NOT NULL
	AND sc.code_type = '1025'
	GROUP BY opportunity_ref
),
job_language AS (
	SELECT opportunity_ref,
	string_agg(l.description, ', ') AS languages
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE opportunity_ref IS NOT NULL
	AND sc.code_type = '1030'
	GROUP BY opportunity_ref
),
job_industry AS (
	SELECT opportunity_ref,
	string_agg(l.description, ', ') AS industries
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE opportunity_ref IS NOT NULL
	AND sc.code_type = '1005'
	GROUP BY opportunity_ref
),
job_type AS (
	SELECT opportunity_ref,
	string_agg(l.description, ', ') AS job_types
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE opportunity_ref IS NOT NULL
	AND sc.code_type = '1010'
	GROUP BY opportunity_ref
),
job_skill AS (
	SELECT opportunity_ref,
	string_agg(l.description, ', ') AS skills
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE opportunity_ref IS NOT NULL
	AND sc.code_type = '1015'
	GROUP BY opportunity_ref
),
job_owner AS (
	SELECT
		person_ref,
		type,
		s.email_address
	FROM person_type pt
	LEFT JOIN staff s ON pt.person_type_ref = s.person_type_ref
	WHERE pt.type LIKE '%Z%'
	AND s.email_address LIKE '%@%'
)

------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
o.opportunity_ref AS "position-externalId",
o.organisation_ref company_id,
COALESCE(cc.contact_id, dc.contact_id::TEXT) AS "position-contactId",
CASE 
	WHEN NULLIF(TRIM(o.displayname), '') IS NOT NULL THEN
		CASE
			WHEN ROW_NUMBER() OVER(PARTITION BY COALESCE(cc.contact_id, dc.contact_id::TEXT), TRIM(LOWER(o.displayname)) ORDER BY COALESCE(cc.contact_id, dc.contact_id::TEXT)) > 1 THEN
					CONCAT(o.displayname, ' ', ROW_NUMBER() OVER(PARTITION BY COALESCE(cc.contact_id, dc.contact_id::TEXT), TRIM(LOWER(o.displayname)) ORDER BY COALESCE(cc.contact_id, dc.contact_id::TEXT)))
			ELSE o.displayname
		END
	ELSE '[No Job Title]' 
END AS "position-title",
to_char(o.date_opened::DATE, 'YYYY-MM-DD') AS "position-startDate",
COALESCE(to_char(o.date_closed::DATE, 'YYYY-MM-DD'), '2019-05-19') AS "position-endDate",
o.no_persons_reqd::int AS "position-headcount",
CASE
	WHEN ot.description = 'Permanent vacancy' THEN 'PERMANENT'
	WHEN ot.description IN ('Temporary vacancy', 'Shift requirement') THEN 'CONTRACT'
	ELSE ot.description
END AS "position-type",
jo.email_address "position-owners",
pv.lower_income salary_from,
pv.upper_income salary_to,
'GBP' AS "position-currency",
CONCAT_WS(
	E'\n',
	CASE WHEN js.description IS NOT NULL THEN CONCAT('Status: ', js.description) END,
	CASE WHEN pv.package IS NOT NULL THEN CONCAT('Package: ', pv.package) END,
	CASE WHEN jl.locations IS NOT NULL THEN CONCAT('Location: ', jl.locations) END,
	CASE WHEN jt.job_types IS NOT NULL THEN CONCAT('Job types: ', jt.job_types) END,
	CASE WHEN o.notes IS NOT NULL THEN CONCAT('Notes: ', E'\n', o.notes) END
) "position-note"

FROM opportunity o
LEFT JOIN contact_company cc ON o.organisation_ref = cc.company_id AND cc.rn = 1
LEFT JOIN generate_default_contact dc ON o.organisation_ref = dc.company_id
LEFT JOIN opportunity_type ot ON o.type = ot.code
LEFT JOIN job_owner jo ON o.responsible_user = jo.person_ref
LEFT JOIN permanent_vac pv ON o.opportunity_ref = pv.opportunity_ref
LEFT JOIN temporary_vac tv ON o.opportunity_ref = tv.opportunity_ref
LEFT JOIN job_status js ON o.record_status = js.code
LEFT JOIN job_status_reason jsr ON o.record_status_reason = jsr.code
LEFT JOIN job_source jso ON o.source = jso.code
LEFT JOIN job_location jl ON o.opportunity_ref = jl.opportunity_ref
LEFT JOIN job_qualification jq ON o.opportunity_ref = jq.opportunity_ref
LEFT JOIN job_language jla ON o.opportunity_ref = jla.opportunity_ref
LEFT JOIN job_industry ji ON o.opportunity_ref = ji.opportunity_ref
LEFT JOIN job_type jt ON o.opportunity_ref = jt.opportunity_ref
LEFT JOIN job_skill jsk ON o.opportunity_ref = jsk.opportunity_ref