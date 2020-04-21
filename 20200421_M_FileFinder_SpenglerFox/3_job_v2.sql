WITH all_contacts AS (
	SELECT cp.idperson
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

, cte_contact_company AS (
	SELECT *
	FROM all_contacts
	WHERE rn = 1
)

--Default contact
, default_contact as (select idassignment
		, idcompany as company_ext_id
		, 'CON' || idcompany as default_con_id
		, 'Default contact' as last_name
		, 'This is default contact for each company' as note
		from "assignment"
		where idcompany not in (select distinct idcompany from company_person where idcompany is not NULL))

--Selected users
, selected_user as (select iduser, idorganizationunit, title, firstname, lastname
		, fullname, replace(useremail, 'spenglerfox.eu', 'spenglerfox.com') as useremail, createdon
		from "user"
		where isdisabled = '0'
		and useremail ilike '%_@_%.__%'
		and (firstname not ilike '%partner%' and jobtitle not ilike '%partner%')
		)

--Assignment owners
, users as (select a.idassignment, a.iduser, u.useremail
		from "assignment" a
		left join selected_user u on u.iduser = a.iduser
		where a.iduser is not NULL
				
		UNION ALL
		
		select a.idassignment, a.iduser, u.useremail
		from assignmentassociate a
		left join selected_user u on u.iduser = a.iduser
		)
		
, assignment_user as (select idassignment
		, string_agg(distinct lower(useremail), ',') as assignment_user
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

--Selected contacts
, cte_contact AS (
	SELECT cp.idperson, cp.idcompany
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
)

--Assignment Contacts | Jobs may link with multiple contacts
, job_contacts as (select ac.idassignmentcontact, ac.idassignment, ac.idperson, ac.createdon::timestamp, ac.modifiedon::timestamp
		, cc.idcompany
		, p.personid, p.firstname, p.lastname, p.emailwork, p.defaultphone
		, a.idcompany as idcompany_origin
		, a.isdeleted
		, a.assignmentno::int
		, row_number() over(partition by ac.idassignment order by ac.modifiedon::timestamp desc, ac.createdon::timestamp desc, p.personid::int desc) rn
		from assignmentcontact ac
		left join "assignment" a on a.idassignment = ac.idassignment
		left join (select * from cte_contact where rn = 1) cc on cc.idperson = ac.idperson
		left join personx p on p.idperson = ac.idperson
		where a.idcompany = cc.idcompany --original company must be same with contact company, otherwise jobs go to default contact
		)
		
--Additional contact info
, job_contact_info as (select idassignment
		, string_agg(distinct
			concat_ws(chr(10)
				, coalesce('[FF ID] ' || nullif(REPLACE(personid, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('[Contact name] ' || firstname || ' ' || lastname, '', NULL)
				, coalesce('[Work email] ' || nullif(REPLACE(emailwork, '\x0d\x0a', ' '), ''), NULL)
				, coalesce('[Default phone] ' || nullif(REPLACE(defaultphone, '\x0d\x0a', ' '), ''), NULL))
		, chr(13)) as job_contact_info
		from job_contacts
		where rn > 1
		group by idassignment
	)
	
--MAIN SCRIPT
SELECT a.idassignment "position-externalId"
	, CASE WHEN f.idassignment IS NULL THEN 'PERMANENT' ELSE 'CONTRACT' END "position-type"
	, a.assignmentno
	, a.idcompany company_id
	, COALESCE(ac.idperson, acs.idperson, dc.default_con_id, '999999999') "position-contactId" --correct contact else pick 1 contact else default contact
	, a.assignmenttitle as "position-title"
	, a.createdon::date "position-startDate" --#inject
	, a.actualcompletedate::date "position-endDate"
	, coalesce(f.numberofpositions::int, 1) "position-headcount"
	, c.value currency
	, a.salaryfrom job_salary_from --#inject
	, a.salaryto job_salary_to --#inject
	, au.assignment_user "position-owners"
	, coalesce(a.estimatedvalue, a.finalfee) "position-actualSalary" --job leads dont have that field
	, a.successprobability pct_chance_of_placement --#inject
	, asd.assignment_document "position-document"
	, concat_ws(chr(10)
		, coalesce('[Assignment External ID] ' || nullif(REPLACE(a.idassignment, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment number] ' || nullif(REPLACE(a.assignmentno::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Created on] ' || nullif(REPLACE(a.createdon, '\x0d\x0a', ' '), ''), NULL) --#inject
		, coalesce('[Assignment Status] ' || nullif(REPLACE(ass.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment Type] ' || nullif(REPLACE(ast.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment Stragedy] ' || nullif(REPLACE(asst.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment Origin] ' || nullif(REPLACE(aso.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Pitch Date] ' || nullif(REPLACE(a.pitchdate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Decision Date] ' || nullif(REPLACE(a.decisiondate, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment comment] ' || chr(10) || nullif(REPLACE(a.assignmentcomment, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Final fee] ' || nullif(REPLACE(a.finalfee::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Fee type] ' || nullif(REPLACE(asf.assignment_fee, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Fee comment] ' || chr(10) || nullif(REPLACE(a.feecomment::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Assignment brief] ' || chr(10) || nullif(REPLACE(REPLACE(a.assignmentbrief, '\x0d\x0a', ' '),'\x09', ' '), ''), NULL)
		, coalesce('[Assigment role location] ' || nullif(REPLACE(arl.assignment_role_location, '\x0d\x0a', ' '), ''), NULL)
		, coalesce(chr(10) || '[Additional contacts]' || chr(10) || nullif(jci.job_contact_info, ''), NULL)
	) "position-note"
FROM (select * from assignment where isdeleted = '0') a
LEFT JOIN (select * from job_contacts where rn = 1) ac ON ac.idassignment = a.idassignment
LEFT JOIN (select * from all_contacts where rn_contact = 1) acs ON acs.company_id = a.idcompany
LEFT JOIN default_contact dc on dc.idassignment = a.idassignment
LEFT JOIN job_contact_info jci on jci.idassignment = a.idassignment
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
WHERE a.isdeleted = '0'
--and a.assignmentno::int in (1004879,1007680,1008886,1011960,1013354,2001160,2001522,2001595,2001616,2001645,2001646,2001647) --jobs migrated
and a.assignmentno::int in ('2000501','2000876','2000906','2000909','2001060','2001066','2001104','2001122','2001124','2001150','2001158','2001177','2001178','2001179','2001188','2001197','2001225','2001227','2001228','2001231','2001235','2001238','2001239','2001265','2001266','2001276','2001286','2001292','2001306','2001328','2001334','2001335','2001340','2001349','2001353','2001359','2001387','2001392','2001393','2001397','2001398','2001400','2001407','2001421','2001423','2001424','2001443','2001446','2001469','2001484','2001485','2001513','2001518','2001521','2001533','2001536','2001537','2001538','2001549','2001553','2001558','2001565','2001567','2001572','2001583','2001593','2001593','2001596','2001602','2001613','2001618','2001623','2001625','2001628','2001631','2001632','2001634','2001639','2001640','2001648','2001655','2001669','2001670','2001671','1013982','1013999','2000700','2000923','2000924','2000960','2001064','2001102','2001103','2001109','2001112','2001154','2001157','2001175','2001221','2001226','2001229','2001230','2001232','2001233','2001234','2001236','2001237','2001240','2001241','2001242','2001243','2001244','2001245','2001248','2001249','2001350','2001364','2001373','2001380','2001390','2001412','2001419','2001422','2001427','2001434','2001460','2001473','2001494','2001496','2001547','2001548','2001550','2001574','2001585','2001591','2001624','2001627','2001630','2001633','2001642','2001650','2001651','2001653','2001659','2001660','2001661','2001662') --jobs leads