SELECT 
a.account_id,
p.account_id parent_id,
a.name company_name,
p.name parent_name
FROM account a
LEFT JOIN
(SELECT DISTINCT account_id, name FROM account) p ON a.parent_id = p.account_id
WHERE p.name is not NULL