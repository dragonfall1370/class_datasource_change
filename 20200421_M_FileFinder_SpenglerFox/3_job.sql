WITH all_contacts AS (
	SELECT cp.idperson contact_id
	, cp.sortorder
	, cp.idcompany company_id
	, px.fullname
	, cp.createdon
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC) rn
	, ROW_NUMBER() OVER(PARTITION BY cp.idcompany ORDER BY cp.createdon ASC) rn_contact
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	LEFT JOIN "user" u ON px.iduser = u.iduser
)
, cte_contact AS (
	SELECT *
	FROM all_contacts
	WHERE rn = 1
)
--Assignment owners
, users as (select a.idassignment, a.iduser, u.useremail
		from "assignment" a
		left join "user" u on u.iduser = a.iduser
		where a.iduser is not NULL
				
		UNION ALL
		
		select a.idassignment, a.iduser, u.useremail
		from assignmentassociate a
		left join "user" u on u.iduser = a.iduser
		)
		
, assignment_user as (select idassignment
		, string_agg(useremail, ',') as assignment_user
		from users
		where useremail is not NULL
		group by idassignment)
		
--Assignment fee type
, assignment_fee as (select idassignment, u.value as assignment_fee
		from assignmentext ae
		left join udfeetype u on u.idudfeetype = ae.idudfeetype
		where ae.idudfeetype is not NULL)

--Role location
, assignment_role_location as (select ac.idassignment
		--, ac.codeid, ur.value
		, string_agg(ur.value, ', ') as assignment_role_location
		from assignmentcode ac
		left join udrolelocation ur on ur.idudrolelocation = ac.codeid
		where ac.idtablemd = 'e2d47b50-404c-451c-b4f2-ab15a0230c6b' --Role Location
		group by ac.idassignment)

--Documents
, documents as (select a.idassignment
		, d.newdocumentname
		, reverse(substring(reverse(d.newdocumentname), 1, position('.' in reverse(d.newdocumentname)))) as extension
		, d.originaldocumentname
		, d.createdon::timestamp as created
		from assignment a
		join entitydocument ed on ed.entityid = a.idassignment
		join document d on d.iddocument = ed.iddocument
		where ed.entityid is not NULL
		and d.newdocumentname is not NULL)
		
, assignment_document as (select idassignment
		, string_agg(newdocumentname, ',' order by created desc) as assignment_document
		from documents
		where extension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')
		group by idassignment)

--MAIN SCRIPT
, cte_job AS (SELECT a.idassignment job_id
	, CASE WHEN f.idassignment IS NULL THEN 'PERMANENT' ELSE 'CONTRACT' END job_type
	, a.idcompany company_id
	, COALESCE(cc.contact_id, acs.contact_id, '999999999') contact_id
	, ac.contactedon
	, a.assignmenttitle as job_title
	, a.createdon::date created_date --#inject
	, coalesce(f.numberofpositions::int, 1) headcount
	, c.value currency
	, a.salaryfrom job_salary_from --#inject
	, a.salaryto job_salary_to --#inject
	, au.assignment_user owner_email
	, a.estimatedstartdate open_date
	, coalesce(a.estimatedvalue, a.finalfee) annual_salary
	, a.successprobability pct_chance_of_placement --#inject
	, ROW_NUMBER() OVER(PARTITION BY COALESCE(cc.contact_id, acs.contact_id), LOWER(a.assignmenttitle) ORDER BY a.createdon ASC) rn_title
	, ROW_NUMBER() OVER(PARTITION BY a.idassignment ORDER BY ac.contactedon ASC) rn
	, asd.assignment_document
	, concat_ws(chr(10)
		, coalesce('[Assignment ID] ' || nullif(REPLACE(a.idassignment, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment number] ' || nullif(REPLACE(a.assignmentno::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Created on] ' || nullif(REPLACE(a.createdon, '\x0d\x0a', ' '), ''), NULL) --#inject
		, coalesce('[Assignment Status] ' || nullif(REPLACE(ass.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment Type] ' || nullif(REPLACE(ast.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment Stragedy] ' || nullif(REPLACE(asst.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Origin] ' || nullif(REPLACE(aso.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Pitch Date] ' || nullif(REPLACE(a.pitchdate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Decision Date] ' || nullif(REPLACE(a.decisiondate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment comment] ' || chr(10) || nullif(REPLACE(a.assignmentcomment, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Estimated value] ' || nullif(REPLACE(a.estimatedvalue::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Estimated fee] ' || nullif(REPLACE(a.estimatedfee::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Final fee] ' || nullif(REPLACE(a.finalfee::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Fee type] ' || nullif(REPLACE(asf.assignment_fee, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Fee comment] ' || chr(10) || nullif(REPLACE(a.feecomment::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment brief] ' || chr(10) || nullif(REPLACE(REPLACE(a.assignmentbrief, '\x0d\x0a', ' '),'\x09', ' '), ''), NULL)
		, coalesce('[Assigment role location] ' || nullif(REPLACE(arl.assignment_role_location, '\x0d\x0a', ' '), ''), NULL)	
		--, coalesce('[Assignment reference] ' || nullif(REPLACE(a.assignmentreference, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Consultant] ' || nullif(REPLACE(abi.consultant, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Consultant fee: ' || nullif(REPLACE(abi.fee, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Is guest assignment contact] ' || case when ntcontact = '1' then 'YES' else 'NO' end, ''), NULL)
		--, coalesce('[Last contacted on] ' || nullif(REPLACE(a.last_contacted_on, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contacted by] ' || nullif(REPLACE(ac.contacted_by, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contacted subject] ' || nullif(REPLACE(ac.contact_subject, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Package comment] ' || nullif(REPLACE(a.package_comment, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Assignment user defined field 1] ' || nullif(REPLACE(ae.ud_field1, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Assignment user defined field 2] ' || nullif(REPLACE(ae.ud_field2, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Flex user defined field 1] ' || nullif(REPLACE(fe.ud_field1, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contract start date] ' || nullif(REPLACE(f.contract_start_date, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contract end date] ' || nullif(REPLACE(f.contract_end_date, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Unit type] ' || nullif(REPLACE(ut.value, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Client rate] ' || nullif(REPLACE(f.client_rate::text, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contractor rate] ' || nullif(REPLACE(f.contractor_rate::text, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Rate comment] ' || nullif(REPLACE(f.rate_comment, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Number of positions] ' || nullif(REPLACE(f.number_of_positions::text, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Base client rate] ' || nullif(REPLACE(f.base_client_rate::text, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Base contractor rate] ' || nullif(REPLACE(f.base_contractor_rate::text, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Assignment contact note] ' || nullif(REPLACE(ac.notes, '\x0d\x0a', ' '), ''), NULL)			
	) note
	FROM assignmentcontact ac
	JOIN (select * from assignment where isdeleted = '0') a ON ac.idassignment = a.idassignment
	LEFT JOIN (select * from cte_contact where rn = 1) cc ON ac.idperson = cc.contact_id
	LEFT JOIN (select * from all_contacts where rn_contact = 1) acs ON acs.company_id = a.idcompany
	LEFT JOIN assignmentstatus ass ON a.idassignmentstatus = ass.idassignmentstatus
	LEFT JOIN assignmenttype ast ON ast.idassignmenttype = a.idassignmenttype
	LEFT JOIN assignmentstrategy asst ON asst.idassignmentstrategy = a.idassignmentstrategy
	LEFT JOIN assignmentorigin aso ON  aso.idassignmentorigin = a.idassignmentorigin
	LEFT JOIN currency c ON c.idcurrency = a.idcurrency
	LEFT JOIN assignment_user au on au.idassignment = a.idassignment
	LEFT JOIN assignment_fee asf on asf.idassignment = a.idassignment
	LEFT JOIN assignment_role_location arl on arl.idassignment = a.idassignment
	LEFT JOIN assignment_document asd on asd.idassignment = a.idassignment
	LEFT JOIN flex f ON f.idassignment = a.idassignment
	--LEFT JOIN "user" u ON u.iduser = a.iduser
	--LEFT JOIN flexext fe ON f.idflex = fe.id_flex
	--LEFT JOIN unittype ut ON f.idunittype = ut.idunittype
	--LEFT JOIN assignmentext ae ON a.idassignment = ae.idassignment
	--LEFT JOIN assignmentbi abi ON a.idassignment = abi.idassignment
	WHERE a.isdeleted = '0'
)

SELECT job_id "position-externalId"
, job_type "position-type"
, contact_id "position-contactId"
, CASE
	WHEN rn_title <> 1 THEN concat(job_title, ' - ', rn_title)
	ELSE job_title END "position-title"
, headcount "position-headcount" 
, owner_email "position-owners"
, currency "position-currency"
, open_date "position-startDate"
, annual_salary "position-actualSalary"
, assignment_document "position-document"
, note "position-note"
FROM cte_job j
WHERE rn = 1 --11187