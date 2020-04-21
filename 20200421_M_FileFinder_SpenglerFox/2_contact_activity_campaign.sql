WITH cte_contact AS (
	SELECT cp.idperson contact_id
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole) rn
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
)

, cte_contact_activity AS (select ctc.contact_id as con_ext_id
	, ale.idcompany1 as com_ext_id
	, al.idactivitylog
	, cc.createdon::timestamp created_date
	, '[Contact Campaign]' || chr(10) ||
			nullif(concat_ws (chr(10)
				, coalesce('Campaign title: ' || nullif(REPLACE(c.campaigntitle, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign contacted by: ' || nullif(REPLACE(cc.contactedby, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign contact subject: ' || nullif(REPLACE(cc.contactsubject, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign description: ' || chr(10) || nullif(REPLACE(c.campaigndescription, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign status: ' || nullif(REPLACE(cs.value, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign approximate number of targets: ' || nullif(REPLACE(c.approximatenooftargets, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign budget: ' || nullif(REPLACE(c.campaignbudget, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign final cost: ' || nullif(REPLACE(c.finalcost, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign created by: ' || nullif(REPLACE(c.createdby, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign created on: ' || nullif(REPLACE(c.createdon, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign modified by: ' || nullif(REPLACE(c.modifiedby, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('Campaign modified on: ' || nullif(REPLACE(c.modifiedon, '\x0d\x0a', ' '), ''), NULL))
			, '') description 
	, cast('-10' as int) as user_account_id
	, 'comment' as category
	, 'contact' as type
	FROM cte_contact ctc
	JOIN activitylogentity ale ON ale.idperson = ctc.contact_id
	JOIN activitylog al ON al.idactivitylog = ale.idactivitylog
	LEFT JOIN (select * from campaigncontact where isexcluded = '0') cc ON cc.idperson = ctc.contact_id
	LEFT JOIN (select * from campaign where isdeleted = '0') c ON c.idcampaign = cc.idcampaign
	LEFT JOIN campaignstatus cs ON c.idcampaignstatus = cs.idcampaignstatus
	WHERE contextentitytype = 'Person'
	AND ctc.rn = 1
)

, distinct_contact_activity AS (
	SELECT *
	, ROW_NUMBER() OVER(PARTITION BY con_ext_id, created_date, description ORDER BY con_ext_id) rn
	FROM cte_contact_activity
)
SELECT *
FROM distinct_contact_activity
WHERE rn = 1
AND description IS NOT NULL
--limit 10

/* AUDIT CONTACT CAMPAIGN
select idcampaign, count(*)
from campaigncontact
group by idcampaign
having count(*) > 1
*/