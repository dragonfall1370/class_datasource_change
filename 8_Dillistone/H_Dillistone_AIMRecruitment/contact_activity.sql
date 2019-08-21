WITH cte_contact AS (
	SELECT
	cp.id_person contact_id,
	ROW_NUMBER() OVER(PARTITION BY cp.id_person ORDER BY cp.sort_order ASC, cp.employment_from DESC, cp.is_default_role) rn
	FROM company_person cp
	LEFT JOIN selected_company sc ON cp.id_company = sc.idcompany
	JOIN person_x px ON cp.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON cp.id_person = p.id_person AND p.is_deleted = 0
),
merge_contact_activity_campaign AS (
	SELECT
	ctc.contact_id,
	al.created_on::timestamp created_date,
	NULLIF(concat(
		CASE WHEN NULLIF(TRIM(REPLACE(al.created_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Created by: ', REPLACE(al.created_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.modified_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Modified on: ', REPLACE(al.modified_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.modified_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Modified by: ', REPLACE(al.modified_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.activity_type, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Activity type: ', REPLACE(al.activity_type, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.start_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task start date: ', REPLACE(t.start_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.completed_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task completed date: ', REPLACE(t.completed_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.due_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task due date: ', REPLACE(t.due_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ts.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task status: ', REPLACE(ts.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(tp.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task priority: ', REPLACE(tp.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task subject: ', REPLACE(t.subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.description, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task description: ', REPLACE(t.description, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.reminder_info, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task reminder info: ', REPLACE(t.reminder_info, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.created_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task created by: ', REPLACE(t.created_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task created on: ', REPLACE(t.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.modified_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task modified by: ', REPLACE(t.modified_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(t.modified_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Task modified on: ', REPLACE(t.modified_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(alct.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Activity log contact type: ', REPLACE(alct.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.progress_table_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Progress table name: ', REPLACE(al.progress_table_name, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cpr.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Progress: ', REPLACE(cpr.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.template, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Template: ', REPLACE(al.template, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Activity subject: ', REPLACE(al.subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.description, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Activity description: ', E'\n', REPLACE(al.description, '\x0d\x0a', ' ')) END
	), '') description,
	cast('-10' as int) as user_account_id,
	'comment' as category,
	'contact' as type
	FROM cte_contact ctc
	JOIN activity_log_entity ale ON ctc.contact_id = ale.id_person
	JOIN activity_log al ON ale.id_activity_log = al.id_activity_log
	LEFT JOIN activity_log_contact_type alct ON al.id_activity_log_contact_type = alct.id_activity_log_contact_type
	LEFT JOIN candidate_progress cpr ON al.progress_id = cpr.id_candidate_progress AND cpr.is_active = 1
	LEFT JOIN task_log tl ON al.id_activity_log = tl.id_activity_log
	LEFT JOIN task t ON tl.id_task = t.id_task
	LEFT JOIN task_status ts ON t.id_task_status = ts.id_task_status
	LEFT JOIN task_priority tp ON t.id_task_priority = tp.id_task_priority
	WHERE context_entity_type = 'Person'
	AND ctc.rn = 1

	UNION ALL

	SELECT
	ctc.contact_id,
	cc.created_on::timestamp created_date,
	NULLIF(concat(
		CASE WHEN NULLIF(TRIM(REPLACE(c.campaign_title, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign title: ', REPLACE(c.campaign_title, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cc.contacted_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign contacted by: ', REPLACE(cc.contacted_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cc.contact_subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign contact subject: ', REPLACE(cc.contact_subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.campaign_description, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign description: ', REPLACE(c.campaign_description, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cs.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign status: ', REPLACE(cs.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.approximate_no_of_targets::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign approximate number of targets: ', REPLACE(c.approximate_no_of_targets::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.campaign_budget::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign budget: ', REPLACE(c.campaign_budget::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.final_cost::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign final cost: ', REPLACE(c.final_cost::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.campaign_value::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign value: ', REPLACE(c.campaign_value::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.created_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign created by: ', REPLACE(c.created_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign created on: ', REPLACE(c.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.modified_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign modified by: ', REPLACE(c.modified_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(c.modified_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Campaign modified on: ', REPLACE(c.modified_on, '\x0d\x0a', ' '), E'\n') END
	), '') description,
	cast('-10' as int) as user_account_id,
	'comment' as category,
	'contact' as type
	FROM cte_contact ctc
	JOIN activity_log_entity ale ON ctc.contact_id = ale.id_person
	JOIN activity_log al ON ale.id_activity_log = al.id_activity_log
	LEFT JOIN campaign_contact cc ON ctc.contact_id = cc.id_person AND cc.is_excluded = 0
	LEFT JOIN campaign c ON cc.id_campaign = c.id_campaign AND c.is_deleted = 0
	LEFT JOIN campaign_status cs ON c.id_campaign_status = cs.id_campaign_status
	WHERE context_entity_type = 'Person'
	AND ctc.rn = 1
),
cte_contact_activity AS (
	SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY contact_id, created_date, description ORDER BY contact_id) rn
	FROM merge_contact_activity_campaign
)

SELECT *
FROM cte_contact_activity
WHERE rn = 1
AND description IS NOT NULL
-- LIMIT 1000
