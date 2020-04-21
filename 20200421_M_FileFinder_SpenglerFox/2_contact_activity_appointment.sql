WITH cte_contact AS (
	SELECT cp.idperson
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole) rn
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
)

, cte_contact_activity AS (SELECT ctc.idperson
	, ale.idcompany1
	, ale.idactivitylog
	, al.createdon::timestamp created_date
	, concat_ws(chr(10), '[Contact appointment]'
		, coalesce('Created by: ' || nullif(REPLACE(al.createdby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Modified on: ' || nullif(REPLACE(al.modifiedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Modified by: ' || nullif(REPLACE(al.modifiedby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity type: ' || nullif(REPLACE(al.activitytype, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment start date: ' || nullif(REPLACE(ap.startdate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment end date: ' || nullif(REPLACE(ap.enddate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment subject: ' || nullif(REPLACE(ap.subject, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment location: ' || nullif(REPLACE(ap.location, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment description: ' || chr(10) || nullif(REPLACE(ap.description, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment created by: ' || nullif(REPLACE(ap.createdby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment created on: ' || nullif(REPLACE(ap.createdon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment modified by: ' || nullif(REPLACE(ap.modifiedby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Appointment modified on: ' || nullif(REPLACE(ap.modifiedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity log contact type: ' || nullif(REPLACE(alct.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Progress table name: ' || nullif(REPLACE(al.progresstablename, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Progress: ' || nullif(REPLACE(cp.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Template: ' || nullif(REPLACE(al.template, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity subject: ' || nullif(REPLACE(al.subject, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('Activity description: ' || chr(10) || nullif(REPLACE(al.description, '\x0d\x0a', ' '), ''), NULL)
	) description
	, cast('-10' as int) as user_account_id
	, 'comment' as category
	, 'contact' as type
	FROM appointment ap
	JOIN appointmentlog apl on apl.idappointment = ap.idappointment
	JOIN activitylog al ON al.idactivitylog = apl.idactivitylog
	JOIN activitylogentity ale ON ale.idactivitylog = al.idactivitylog
	JOIN cte_contact ctc ON ctc.idperson = ale.idperson
	LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
	LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
	WHERE 1=1
	and ctc.rn = 1
	--and ale.contextentitytype = 'Person'
) 
--select count(*) from cte_contact_activity
, distinct_contact_activity AS (
	SELECT *
	, ROW_NUMBER() OVER(PARTITION BY idperson, created_date, description ORDER BY idperson) rn
	FROM cte_contact_activity
)
SELECT idperson con_ext_id
, idcompany1 com_ext_id
, idactivitylog
, created_date
, description
, user_account_id
, category
, type
FROM distinct_contact_activity
WHERE rn = 1
AND description IS NOT NULL
--limit 10