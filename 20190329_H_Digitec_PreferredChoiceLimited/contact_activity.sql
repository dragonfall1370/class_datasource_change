select
CONVERT(VARCHAR(MAX), contact_id) contact_id,
cast('-10' as int) as user_account_id,
'comment' as 'category',
'contact' as 'type',
COALESCE(date_time, CURRENT_TIMESTAMP) created_on,
concat(
	concat('Contact name: ', contact_name, CHAR(10)),
	concat('Recorded by: ', recorded_by, CHAR(10)),
	concat('Content:', char(10),log_entry)
) content
from tbl_comp_contact_log
where contact_id in (select distinct field2 from [Company Contact Database])