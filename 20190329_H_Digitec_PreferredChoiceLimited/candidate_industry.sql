with cte_industry as (
SELECT 
candidate_id, 
field14,
case
	when TRIM(Split.a.value('.', 'VARCHAR(100)')) like '%**%' then SUBSTRING(TRIM(Split.a.value('.', 'VARCHAR(100)')), 1, CHARINDEX('*', TRIM(Split.a.value('.', 'VARCHAR(100)'))) - 1)
	when TRIM(Split.a.value('.', 'VARCHAR(100)')) like '%--%' then SUBSTRING(TRIM(Split.a.value('.', 'VARCHAR(100)')), 1, CHARINDEX('--', TRIM(Split.a.value('.', 'VARCHAR(100)'))) - 1)
	when TRIM(Split.a.value('.', 'VARCHAR(100)')) like '%DO NOT%' then SUBSTRING(TRIM(Split.a.value('.', 'VARCHAR(100)')), 1, CHARINDEX('DO NOT', TRIM(Split.a.value('.', 'VARCHAR(100)'))) - 1)
	when TRIM(Split.a.value('.', 'VARCHAR(100)')) like '%His contract at Virgin%' then SUBSTRING(TRIM(Split.a.value('.', 'VARCHAR(100)')), 1, CHARINDEX('His contract at Virgin', TRIM(Split.a.value('.', 'VARCHAR(100)'))) - 1)
	when TRIM(Split.a.value('.', 'VARCHAR(100)')) like '%UNDER OFFER FROM SCHAWK%' then SUBSTRING(TRIM(Split.a.value('.', 'VARCHAR(100)')), 1, CHARINDEX('UNDER OFFER FROM SCHAWK', TRIM(Split.a.value('.', 'VARCHAR(100)'))) - 3)
	else replace(replace(TRIM(Split.a.value('.', 'VARCHAR(100)')), '|', ''), '.', '')
end industry
FROM (SELECT field2 candidate_id, field14, CAST ('<M>' + REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), field14),',,',','),',','</M><M>') + '</M>' AS XML) AS Data FROM [Candidates Database]) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
)

select
candidate_id,
industry,
current_timestamp as insert_timestamp
from cte_industry
where industry <> ''