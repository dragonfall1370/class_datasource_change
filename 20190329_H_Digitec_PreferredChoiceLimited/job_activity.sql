with cte_merge_jobs as (
	select *, 
	'PERMANENT' type 
	from [Permanent Placement Table]
	UNION ALL
	select *, 
	'CONTRACT' type 
	from [Temporary Booking Table]
)
select 
field2 job_id,
cast('-10' as int) as user_account_id,
'comment' as 'category',
'job' as 'type',
COALESCE(field3, CURRENT_TIMESTAMP) created_on,
[Candidates Action History] content
from cte_merge_jobs