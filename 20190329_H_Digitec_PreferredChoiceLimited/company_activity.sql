SELECT
CONVERT(VARCHAR(MAX), ccl.comp_id) company_id,
cast('-10' as int) as user_account_id,
'comment' as 'category',
'company' as 'type',
COALESCE(ccl.date_time, ccl.date, CURRENT_TIMESTAMP) created_on,
concat(
	concat('Contact name: ', ccl.contact_name, CHAR(10)),
	concat('Recorded by: ', ccl.recorded_by, CHAR(10)),
	concat('Content:', char(10), ccl.log_entry)
) content
from tbl_comp_contact_log ccl
JOIN [Company Database] cd ON cd.Field2 = ccl.comp_id

UNION ALL

SELECT
CONVERT(VARCHAR(MAX), field2) company_id,
cast('-10' as int) as user_account_id,
'comment' as 'category',
'company' as 'type',
COALESCE(field16, CURRENT_TIMESTAMP) created_on,
concat('-----------------------------------------', CHAR(10), 
'CANDIDATE ACTION HISTORY', CHAR(10), 
'-----------------------------------------', CHAR(10),
[Candidates Action History]) content
from [Company Database]
WHERE [Candidates Action History] IS NOT NULL

