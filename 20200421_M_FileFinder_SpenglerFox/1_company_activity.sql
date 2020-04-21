/* BEFORE RUNNING, ADD TEMP COLUMNS
alter table activity
add column activity_ext_id character varying (100) 
*/

WITH cte_contact AS (
	SELECT cp.idperson contact_id
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole) rn
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
)

, cte_candidate AS (
	SELECT c.idperson candidate_id
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
)

, cte_company_activity AS (SELECT c.idcompany
	, ale.idactivitylog
	, al.createdon::timestamp created_date
	--, case when alct.value in ('Assignment Candidate', 'Introduction Candidate') then cand.candidate_id
	--	else NULL end as cand_ext_id
	, case when alct.value not in ('Assignment Candidate', 'Introduction Candidate') then con.contact_id
		else NULL end as con_ext_id
	, concat_ws(chr(10), '[Company log]'
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
	, 'company' as type
	FROM activitylogentity ale
	JOIN company c ON c.idcompany = ale.contextentityid
	JOIN activitylog al ON al.idactivitylog = ale.idactivitylog
	LEFT JOIN (select * from cte_contact where rn = 1) con on con.contact_id = ale.idperson
	--LEFT JOIN (select * from cte_candidate where rn = 1) cand on cand.candidate_id = ale.idperson
	LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
	LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
	LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
	LEFT JOIN task t ON tl.idtask = t.idtask
	LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
	LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
	WHERE ale.contextentitytype = 'Company'
) 
, distinct_company_activity AS (
	SELECT *
	, ROW_NUMBER() OVER(PARTITION BY idcompany, created_date, description ORDER BY idcompany) rn
	FROM cte_company_activity
)
SELECT idcompany com_ext_id
, idactivitylog
, created_date
, con_ext_id
, description
, user_account_id
, category
, type
FROM distinct_company_activity
WHERE rn = 1
--and idactivitylog = 'abf2df63-44d7-4c5f-8fd1-232e02f000e9'
limit 100