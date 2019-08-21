SELECT
a.account_id company_id,
t.description,
t.created_date::TIMESTAMP,
cast('-10' as int) as user_account_id,
'comment' as category,
'company' as type
FROM account a
JOIN task t ON a.account_id = t.account_id
WHERE t.is_deleted = 0
ORDER BY a.account_id