select
concat(
	upper('Call Logs')
	, E'\n'
	, lpad('', length('Call Logs'), '-')
	, E'\n'
	, content
) as content1
from activity
where
	user_account_id = -10
	and candidate_id in (
	select id from candidate c
	where c.external_id is not null
		and c.external_id <> '0'
)

-- update activity
-- set content =
-- concat(
-- 	upper('Call Logs')
-- 	, E'\n'
-- 	, lpad('', length('Call Logs'), '-')
-- 	, E'\n'
-- 	, content
-- )
-- where
-- 	user_account_id = -10
-- 	and candidate_id in (
-- 	select id from candidate c
-- 	where c.external_id is not null
-- 		and c.external_id <> '0'
-- )