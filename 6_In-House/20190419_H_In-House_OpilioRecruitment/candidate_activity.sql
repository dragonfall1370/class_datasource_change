SELECT
a.candidate_id,
CONCAT_WS(
	CHAR(10),
	CASE WHEN NULLIF(category, '') IS NOT NULL THEN CONCAT('Category: ', category) END,
	CASE WHEN NULLIF(type, '') IS NOT NULL THEN CONCAT('Type: ', type) END,
	CASE WHEN NULLIF(stage, '') IS NOT NULL THEN CONCAT('Stage: ', stage) END,
	CASE WHEN NULLIF(start, '') IS NOT NULL THEN CONCAT('Start date: ', start) END,
	CASE WHEN NULLIF(end, '') IS NOT NULL THEN CONCAT('End date: ', end) END,
	CASE WHEN NULLIF(status, '') IS NOT NULL THEN CONCAT('Status: ', status) END,
	CASE WHEN NULLIF(jo.roleTitle, '') IS NOT NULL THEN CONCAT('Job opening: ', roleTitle) END,
	CASE WHEN NULLIF(comments, '') IS NOT NULL THEN CONCAT('Comment: ', comments) END
) content,
COALESCE(a.created, CURRENT_TIMESTAMP) inserted_timestamp,
-10 as user_account_id,
'comment' as 'category',
'candidate' as 'type'

FROM alpha_activity a
LEFT JOIN alpha_job_opening jo ON a.jobOpening_id = jo.id