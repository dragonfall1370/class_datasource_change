WITH cte_candidate AS (
	SELECT c.idperson
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
)

, cte_contact_activity AS (SELECT ctc.idperson
	, ale.idactivitylog
	, al.createdon::timestamp created_date
	, concat_ws(chr(10), '[Candidate log]'
		, coalesce('Created by: ' || nullif(REPLACE(al.createdby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Modified on: ' || nullif(REPLACE(al.modifiedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Modified by: ' || nullif(REPLACE(al.modifiedby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity type: ' || nullif(REPLACE(al.activitytype, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task start date: ' || nullif(REPLACE(t.startdate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task completed date: ' || nullif(REPLACE(t.completeddate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task due date: ' || nullif(REPLACE(t.duedate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task status: ' || nullif(REPLACE(ts.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task priority: ' || nullif(REPLACE(tp.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task subject: ' || nullif(REPLACE(t.subject, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task description: ' || chr(10) || nullif(REPLACE(t.description, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task reminder info: ' || nullif(REPLACE(t.reminderinfo, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task created by: ' || nullif(REPLACE(t.createdby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task created on: ' || nullif(REPLACE(t.createdon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task modified by: ' || nullif(REPLACE(t.modifiedby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Task modified on: ' || nullif(REPLACE(t.modifiedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity log contact type: ' || nullif(REPLACE(alct.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Progress table name: ' || nullif(REPLACE(al.progresstablename, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Progress: ' || nullif(REPLACE(cp.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Template: ' || nullif(REPLACE(al.template, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity subject: ' || nullif(REPLACE(al.subject, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity description: ' || chr(10) || nullif(REPLACE(al.description, '\x0d\x0a', ' '), ''), NULL)
	) description
	, cast('-10' as int) as user_account_id
	, 'comment' as category
	, 'candidate' as type
	FROM activitylogentity ale
	JOIN (select * from cte_candidate where rn = 1) ctc ON ctc.idperson = ale.contextentityid
	JOIN activitylog al ON al.idactivitylog = ale.idactivitylog
	LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
	LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
	LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
	LEFT JOIN task t ON tl.idtask = t.idtask
	LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
	LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
	WHERE 1=1
	and ale.contextentitytype = 'Person'
	and alct.value in ('Introduction Candidate', 'General Contact', 'Assignment Candidate', 'Reference', 'None', 'Assignment Source')
) 

, distinct_contact_activity AS (
	SELECT *
	, ROW_NUMBER() OVER(PARTITION BY idperson, created_date, description ORDER BY idperson) rn
	FROM cte_contact_activity
)
SELECT idperson cand_ext_id
, idactivitylog
, created_date
, description
, user_account_id
, category
, type
FROM distinct_contact_activity
WHERE rn = 1
AND description IS NOT NULL
limit 10