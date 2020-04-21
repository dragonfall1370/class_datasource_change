select idassignmentsource
, asr.idassignment job_ext_id
, asr.idperson cand_ext_id
, concat_ws(chr(10), '[Assignment source]'
	, coalesce('Contacted by: ' || nullif(REPLACE(asr.contactedby, '\x0d\x0a', ' '), ''))
	, coalesce('Contacted on: ' || nullif(REPLACE(asr.contactedon, '\x0d\x0a', ' '), ''))
	, coalesce('Modified on: ' || nullif(REPLACE(asr.modifiedon, '\x0d\x0a', ' '), ''))
	, coalesce('Assignment Source progress: ' || nullif(REPLACE(asp.value, '\x0d\x0a', ' '), ''))
	, coalesce('Contact subject: ' || nullif(REPLACE(asr.contactsubject, '\x0d\x0a', ' '), ''))
	, coalesce('Notes: ' || nullif(REPLACE(asr.notes, '\x0d\x0a', ' '), ''))
	) activity_comment
, asr.modifiedon::timestamp created_date
, cast('-10' as int) as user_account_id
, 'comment' as category
, 'job' as type
from assignmentsource asr
left join assignmentsourceprogress asp on asp.idassignmentsourceprogress = asr.idassignmentsourceprogress