WITH all_contacts AS (
	SELECT
	cp.id_person contact_id,
	cp.sort_order,
	cp.id_company company_id,
	px.full_name,
	cp.created_on,
	ROW_NUMBER() OVER(PARTITION BY cp.id_person ORDER BY cp.sort_order ASC) rn,
	ROW_NUMBER() OVER(PARTITION BY cp.id_company ORDER BY cp.created_on ASC) rn_contact
	FROM company_person cp
	LEFT JOIN selected_company sc ON cp.id_company = sc.idcompany
	JOIN person_x px ON cp.id_person = px.id_person AND px.is_deleted = 0
	LEFT JOIN "user" u ON px.id_user = u.id_user
),
cte_contact AS (
	SELECT *
	FROM all_contacts
	WHERE rn = 1
),
cte_job AS (
	SELECT
	a.id_assignment job_id,
	ROW_NUMBER() OVER(PARTITION BY a.id_assignment ORDER BY ac.contacted_on ASC) rn
	FROM assignment_contact ac
	JOIN "assignment" a ON ac.id_assignment = a.id_assignment AND a.is_deleted = 0
	LEFT JOIN cte_contact cc ON ac.id_person = cc.contact_id AND cc.rn = 1
	LEFT JOIN all_contacts acs ON acs.company_id = a.id_company AND acs.rn_contact = 1
)
SELECT
cj.job_id,
al.created_on::timestamp created_date,
concat(
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
) description,
cast('-10' as int) as user_account_id,
'comment' as category,
'job' as type
FROM cte_job cj
JOIN activity_log_entity ale ON cj.job_id = ale.context_entity_id
JOIN activity_log al ON ale.id_activity_log = al.id_activity_log
LEFT JOIN activity_log_contact_type alct ON al.id_activity_log_contact_type = alct.id_activity_log_contact_type
LEFT JOIN candidate_progress cpr ON al.progress_id = cpr.id_candidate_progress AND cpr.is_active = 1
LEFT JOIN task_log tl ON al.id_activity_log = tl.id_activity_log
LEFT JOIN task t ON tl.id_task = t.id_task
LEFT JOIN task_status ts ON t.id_task_status = ts.id_task_status
LEFT JOIN task_priority tp ON t.id_task_priority = tp.id_task_priority
WHERE context_entity_type IN ('Assignment', 'Flex')
AND cj.rn = 1
-- LIMIT 1000