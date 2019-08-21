WITH cte_company_activity AS (
	SELECT
	sc.idcompany company_id,
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
		CASE WHEN NULLIF(TRIM(REPLACE(cp.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Progress: ', REPLACE(cp.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.template, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Template: ', REPLACE(al.template, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Activity subject: ', REPLACE(al.subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(al.description, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Activity description: ', E'\n', REPLACE(al.description, '\x0d\x0a', ' ')) END
	) description,
	cast('-10' as int) as user_account_id,
	'comment' as category,
	'company' as type
	FROM selected_company sc
	JOIN company c ON sc.idcompany = c.id_company
	JOIN activity_log_entity ale ON sc.idcompany = ale.id_company1
	JOIN activity_log al ON ale.id_activity_log = al.id_activity_log
	LEFT JOIN activity_log_contact_type alct ON al.id_activity_log_contact_type = alct.id_activity_log_contact_type
	LEFT JOIN candidate_progress cp ON al.progress_id = cp.id_candidate_progress AND cp.is_active = 1
	LEFT JOIN task_log tl ON al.id_activity_log = tl.id_activity_log
	LEFT JOIN task t ON tl.id_task = t.id_task
	LEFT JOIN task_status ts ON t.id_task_status = ts.id_task_status
	LEFT JOIN task_priority tp ON t.id_task_priority = tp.id_task_priority
	WHERE context_entity_type = 'Company'
),
distinct_company_activity AS (
	SELECT *,	ROW_NUMBER() OVER(PARTITION BY company_id, created_date, description ORDER BY company_id) rn
	FROM cte_company_activity
)
SELECT *
FROM distinct_company_activity
WHERE rn = 1
-- LIMIT 1000