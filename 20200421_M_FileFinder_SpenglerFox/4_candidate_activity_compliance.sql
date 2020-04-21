WITH cte_candidate AS (
	SELECT c.idperson
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
)

, cte_candidate_activity AS (
	SELECT cl.idcompliancelog
	, cl.idperson candidate_id
	, cl.createdon::timestamp created_date
	, concat_ws(chr(10)
			, coalesce('[Candidate compliance]' || chr(10) || 'Created by: ' || nullif(REPLACE(cl.createdby, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Created on: ' || nullif(REPLACE(cl.createdon, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Processing reason value: ' || nullif(REPLACE(cl.processingreasonvalue, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Processing status: ' || nullif(REPLACE(pst.value, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Result: ' || nullif(REPLACE(cl.result, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('GT Error code: ' || nullif(REPLACE(cl.errorcode, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('GT Error description: ' || chr(10) || nullif(REPLACE(cl.errordescription, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Person title: ' || nullif(REPLACE(cl.persontitle, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('First name: ' || nullif(REPLACE(cl.firstname, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Last name: ' || nullif(REPLACE(cl.lastname, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Email: ' || nullif(REPLACE(cl.email, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Person communication type: ' || nullif(REPLACE(pct.value, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Email template: ' || nullif(REPLACE(cl.emailtemplate, '\x0d\x0a', ' '), ''), NULL)
			--, coalesce('Processing reason by: ' || nullif(REPLACE(px.processingreasonby, '\x0d\x0a', ' '), ''), NULL)
			--, coalesce('Processing reason on: ' || nullif(REPLACE(px.processingreasonon, '\x0d\x0a', ' '), ''), NULL)
			--, coalesce('Processing reason log: ' || nullif(REPLACE(px.processingreasonlog, '\x0d\x0a', ' '), ''), NULL)
			--, coalesce('Reason log: ' || chr(10) || nullif(REPLACE(REPLACE(cl.reasonlog, '\x0d\x0a', ' '), '\x0a', ' '), ''), NULL)
	) description --gdpr_note
	, cast('-10' as int) as user_account_id
	, 'comment' as category
	, 'candidate' as type
	FROM compliancelog cl
	JOIN (select * from cte_candidate where rn = 1) c on c.idperson = cl.idperson
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
	LEFT JOIN processingstatus pst ON pst.idprocessingstatus = px.idprocessingstatus_string
	LEFT JOIN personcommunicationtype pct ON cl.idpersoncommunicationtype = pct.idpersoncommunicationtype
)

SELECT candidate_id as cand_ext_id
, created_date
, description
, user_account_id
, category
, type
FROM cte_candidate_activity
WHERE description <> ''
-- LIMIT 100