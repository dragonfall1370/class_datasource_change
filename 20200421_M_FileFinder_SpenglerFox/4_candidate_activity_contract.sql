select c.idcontract --activity_ext_id
	, c.idperson as cand_ext_id
	, f.idassignment as job_ext_id
	, c.createdon::timestamp created_date
	, concat_ws(chr(10), '[Candidate contract]'
			, coalesce('Created by: ' || nullif(REPLACE(c.createdby, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Created on: ' || nullif(REPLACE(c.createdon, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Modified on: ' || nullif(REPLACE(c.modifiedon, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Modified by: ' || nullif(REPLACE(c.modifiedby, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Contract type: ' || nullif(REPLACE(ct.value, '\x0d\x0a', ' '), ''), NULL) --c.idcontracttype
			, coalesce('Contract status: ' || nullif(REPLACE(cs.value, '\x0d\x0a', ' '), ''), NULL) --c.idcontractstatus
			, coalesce('Contract reference: ' || nullif(REPLACE(c.contractreference, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Contract job title: ' || nullif(REPLACE(c.contractjobtitle, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Contract start date: ' || nullif(REPLACE(c.contractstartdate, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Estimated contract end date: ' || nullif(REPLACE(c.estimatedcontractenddate, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Contract extended to date: ' || nullif(REPLACE(c.contractextendedtodate, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Contract address: ' || nullif(REPLACE(c.contractaddress, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Contract hours per day: ' || nullif(REPLACE(c.contracthoursperday, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('Contract note: ' || chr(10) || nullif(REPLACE(c.contractnote, '\x0d\x0a', ' '), ''), NULL)
		) description
	, cast('-10' as int) as user_account_id
	, 'comment' as category
	, 'candidate' as type
from contract c
left join flex f on f.idflex = c.idflex
left join contracttype ct on ct.idcontracttype = c.idcontracttype
left join contractstatus cs on cs.idcontractstatus = c.idcontractstatus

/* AUDIT JOB LOG
--No multiple job
select idassignment, count(*)
from flex
group by idassignment
having count(*) > 1
*/