WITH split_relocate_location_list AS (
	SELECT 
	id_person, 
	s.relocate_location
	FROM person_x px, UNNEST(string_to_array(px.id_relocate_location_string_list, ',')) s(relocate_location)
),
relocate_location AS (
	SELECT
	id_person,
	l.value relocate_location
	FROM split_relocate_location_list srll
	LEFT JOIN "location" l ON srll.relocate_location = l.id_location
),
cte_join_relocate_location_list AS (
	SELECT 
	id_person, 
	string_agg(relocate_location, ', ') relocate_location_list
	FROM relocate_location 
	GROUP BY id_person
),
cte_contact AS (
	SELECT
	cp.id_person contact_id,
	cp.sort_order,
	sc.idcompany company_id,
	TRIM(px.first_name) first_name,
	TRIM(px.middle_name) middle_name,
	TRIM(px.last_name) last_name,
	ROW_NUMBER() OVER(PARTITION BY cp.id_person ORDER BY cp.sort_order ASC, cp.employment_from DESC, cp.is_default_role) rn,
	px.known_as preferred_name,
	px.default_phone phone,
	px.email_work primary_email,
	px.email_private personal_email,
	CASE
		WHEN px.email_work IS NOT NULL THEN ROW_NUMBER() OVER(PARTITION BY LOWER(px.email_work) ORDER BY cp.sort_order ASC, cp.employment_from DESC, cp.is_default_role = 1) 
		ELSE 1
	END rn_email,
	px.job_title title,
	u.user_email owner_email,
	concat(
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_id::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact id: ', REPLACE(px.person_id::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.maiden_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Maiden name: ', REPLACE(px.maiden_name, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.date_of_birth, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Date of birth: ', REPLACE(px.date_of_birth, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.family, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Family: ', REPLACE(px.family, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Created on: ', REPLACE(px.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.alert_text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Alert text: ', REPLACE(px.alert_text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN px.is_off_limit = 0 THEN concat('Is off limit: No', E'\n') ELSE concat('Is off limit: Yes', E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(olt.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit type: ', REPLACE(olt.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_date_from, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit from: ', REPLACE(pol.off_limit_date_from, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_date_to, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit to: ', REPLACE(pol.off_limit_date_to, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_note, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit note: ', REPLACE(pol.off_limit_note, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perrt.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation type: ', REPLACE(perrt.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perr.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation created on: ', REPLACE(perr.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perr.created_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation created by: ', REPLACE(perr.created_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perr.relation_description, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation description: ', REPLACE(perr.relation_description, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_reference, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact reference: ', REPLACE(px.person_reference, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.company_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company name: ', REPLACE(px.company_name, '\x0d\x0a', ' '), E'\n') END, 
		CASE WHEN NULLIF(TRIM(REPLACE(pet.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Preferred employment type: ', REPLACE(pet.value, '\x0d\x0a', ' '), E'\n') END, 
		CASE WHEN NULLIF(TRIM(REPLACE(px.address_default_full, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Full default address: ', REPLACE(px.address_default_full, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(co.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default country: ', REPLACE(co.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.address_default_post_code, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default postcode: ', REPLACE(px.address_default_post_code, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.default_url, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default URL: ', REPLACE(px.default_url, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.direct_line_phone, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Direct line phone: ', REPLACE(px.direct_line_phone, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.mobile_business, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Business mobile: ', REPLACE(px.mobile_business, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.mobile_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Other mobile: ', REPLACE(px.mobile_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.phone_home, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Home phone: ', REPLACE(px.phone_home, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.phone_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Other phone: ', REPLACE(px.phone_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.default_email, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default Email: ', REPLACE(px.default_email, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.email_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Other email: ', REPLACE(px.email_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cur.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contractor unavailable reason: ', REPLACE(cur.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.international_value_string, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('International: ', REPLACE(px.international_value_string, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(l.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Location: ', REPLACE(l.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.nationality_value_string, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Nationality: ', REPLACE(px.nationality_value_string, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(po.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact origin: ', REPLACE(po.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ps.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact status: ', REPLACE(ps.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pc.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Previous candidate: ', REPLACE(pc.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.qualification_value_string, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Qualification: ', REPLACE(px.qualification_value_string, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(jrll.relocate_location_list, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Relocate location: ', REPLACE(jrll.relocate_location_list, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(r.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Relocate: ', REPLACE(r.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ut.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Unit type: ', REPLACE(ut.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.market_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Market rate: ', REPLACE(px.market_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.minimum_required_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Minimum required rate: ', REPLACE(px.minimum_required_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.owning_user, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Owning user: ', REPLACE(px.owning_user, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.package::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Package: ', REPLACE(px.package::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_company, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Previous company: ', REPLACE(px.previous_company, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_job_title, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Previous job_title: ', REPLACE(px.previous_job_title, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.salary::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Salary: ', REPLACE(px.salary::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(p.contact_subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact subject: ', REPLACE(p.contact_subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(p.contacted_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contacted by: ', REPLACE(p.contacted_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(p.contacted_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contacted on: ', REPLACE(p.contacted_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.note, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact note: ', E'\n', REPLACE(px.note, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_comment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact comment: ', E'\n', REPLACE(px.person_comment, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.job_notes, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Job note: ', E'\n', REPLACE(REPLACE(px.job_notes, '\x0d\x0a', ' '), '\x0a', ' '), E'\n') END,
		--GDPR
		CASE WHEN COALESCE(TRIM(REPLACE(px.compliance_start_date, '\x0d\x0a', '')), TRIM(REPLACE(px.compliance_end_date, '\x0d\x0a', '')), TRIM(REPLACE(pr.value, '\x0d\x0a', '')), TRIM(REPLACE(pst.value, '\x0d\x0a', '')), TRIM(REPLACE(px.processing_reason_by, '\x0d\x0a', '')), TRIM(REPLACE(px.processing_reason_on, '\x0d\x0a', '')), TRIM(REPLACE(px.processing_reason_on, '\x0d\x0a', '')), TRIM(REPLACE(px.processing_reason_log, '\x0d\x0a', ''))) = '' THEN ''
		ELSE concat('--------------------', E'\n', '       GDPR Notes', E'\n', '--------------------', E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.compliance_start_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Compliance start date: ', REPLACE(px.compliance_start_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.compliance_end_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Compliance end date: ', REPLACE(px.compliance_end_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pr.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason: ', REPLACE(pr.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pst.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing status: ', REPLACE(pst.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.processing_reason_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason by: ', REPLACE(px.processing_reason_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.processing_reason_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason on: ', REPLACE(px.processing_reason_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.processing_reason_log, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason log: ', REPLACE(px.processing_reason_log, '\x0d\x0a', ' '), E'\n') END
	) note
	-- d.new_document_name contact_document
	FROM company_person cp
	LEFT JOIN selected_company sc ON cp.id_company = sc.idcompany
	JOIN person_x px ON cp.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON cp.id_person = p.id_person AND p.is_deleted = 0
	LEFT JOIN "user" u ON px.id_user = u.id_user
	LEFT JOIN country co ON px.address_default_id_country_string = co.id_country
	LEFT JOIN contractor_unavailable_reason cur ON px.id_contractor_unavailable_reason_string = cur.id_contractor_unavailable_reason
	LEFT JOIN "location" l ON px.id_location_string = l.id_location
	LEFT JOIN person_origin po ON px.id_person_origin_string = po.id_person_origin
	LEFT JOIN person_status ps ON px.id_person_status_string = ps.id_person_status
	LEFT JOIN previous_candidate pc ON px.id_previous_candidate_string = pc.id_previous_candidate
	LEFT JOIN processing_reason pr ON px.id_processing_reason_string = pr.id_processing_reason
	LEFT JOIN processing_status pst ON px.id_processing_status_string = pst.id_processing_status
	LEFT JOIN relocate r ON px.id_relocate_string = r.id_relocate
	LEFT JOIN cte_join_relocate_location_list jrll ON px.id_person = jrll.id_person
	LEFT JOIN unit_type ut ON px.id_unit_type_string = ut.id_unit_type
	LEFT JOIN preferred_employment_type pet ON px.id_preferred_employment_type_string = pet.id_preferred_employment_type
	LEFT JOIN person_off_limit pol ON px.id_person = pol.id_person
	LEFT JOIN off_limit_type olt ON pol.id_off_limit_type = olt.id_off_limit_type
	LEFT JOIN person_relation perr ON px.id_person = perr.id_person
	LEFT JOIN person_relation_type perrt ON perr.id_person_relation_type = perrt.id_person_relation_type
)

SELECT
contact_id "contact-externalId",
COALESCE(company_id, '1') "contact-companyId",
first_name "contact-firstName",
middle_name "contact-middleName",
last_name "contact-lastName",
phone "contact-phone",
CASE
	WHEN rn_email <> 1 THEN OVERLAY(primary_email PLACING concat('DUP_', CASE WHEN rn_email = 2 THEN '' ELSE rn_email::text END, '_') from 1 for 0)
	ELSE primary_email
END "contact-email",
personal_email,
title "contact-jobTitle",
owner_email "contact-owners",
note "contact-Note"


FROM cte_contact
WHERE rn = 1
