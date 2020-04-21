WITH dup as (select idperson, TRIM('' FROM (TRIM('''' FROM translate(emailwork, ':?!\/#$%^&*()<>{}[]', '')))) as emailwork
	--distinct email if emails exist more than once
	, row_number() over(partition by trim(' ' from lower(translate(emailwork, ':?!\/#$%^&*()<>{}[]', ''))) order by idperson asc) as rn
	--distinct if contacts may have more than 1 email
	, row_number() over(partition by idperson order by trim(' ' from emailwork)) as contactrn
	from personx
	where emailwork like '%_@_%.__%')

, cte_contact AS (
	SELECT cp.idperson, cp.idcompany
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON cp.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
)

--Documents
, documents as (select c.idcandidate, c.idperson
		, d.newdocumentname
		, reverse(substring(reverse(d.newdocumentname), 1, position('.' in reverse(d.newdocumentname)))) as extension
		, d.originaldocumentname
		, d.createdon::timestamp as created
		from candidate c
		join entitydocument ed on ed.entityid = c.idperson
		join document d on d.iddocument = ed.iddocument
		where ed.entityid is not NULL
		and d.newdocumentname is not NULL)
		
, candidate_document as (select idperson
		, string_agg(newdocumentname, ',' order by created desc) as candidate_document
		from documents
		where extension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')
		group by idperson)

--Selected users
, selected_user as (select iduser, idorganizationunit, title, firstname, lastname
		, fullname, replace(useremail, 'spenglerfox.eu', 'spenglerfox.com') as useremail, createdon
		from "user"
		where isdisabled = '0'
		and useremail ilike '%_@_%.__%')
		
--MAIN SCRIPT
SELECT cp.idperson "contact-externalId"
	, case when cp.idcompany in (select idcompany from company) then cp.idcompany
		else '999999999' end as "contact-companyId"
	, TRIM(REPLACE(px.firstname, '\x0d\x0a', ' ')) "contact-firstName"
	, TRIM(REPLACE(px.middlename, '\x0d\x0a', ' ')) "contact-middleName"
	, coalesce(nullif(TRIM(px.lastname), ''), 'Last name ' || cp.idperson) "contact-lastName"
	, REPLACE(px.knownas, '\x0d\x0a', ' ') preferred_name
	, coalesce(nullif(directlinephone,''), nullif(px.defaultphone,'')) "contact-phone"
	, concat_ws(',', nullif(mobileprivate,''), nullif(px.mobilebusiness,'')) mobile --#inject
	, case when dup.rn <> 1 then dup.rn || '_' || dup.emailwork 
		else nullif(dup.emailwork, '') end "contact-email"
	, TRIM('' FROM (TRIM('''' from translate(px.emailprivate, ':?!\/#$%^&*()<>{}[]', '')))) personal_email --#Inject
	, px.jobtitle "contact-jobTitle"
	, u.useremail "contact-owners"
	, p.dateofbirth as dob --#inject
	, p.createdon as reg_date --#inject
	, case t.value
		when 'Miss' then 'Miss.'
		when 'Ms' then 'Ms.'
		when 'Dr' then 'Dr.'
		when 'Mr' then 'Mr.'
		when 'Mrs' then 'Mrs.'
		else NULL end gender_title --#inject
	--, concat_ws(chr(10)
	--	, coalesce('[Contact External ID] ' || nullif(REPLACE(px.idperson, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Contact ID] ' || nullif(REPLACE(px.personid, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Contact status] ' || nullif(REPLACE(ps.value, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Created by] ' || nullif(REPLACE(p.createdby, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Modified by] ' || nullif(REPLACE(p.modifiedby, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Modified on] ' || nullif(REPLACE(p.modifiedon, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce(chr(10) || nullif(REPLACE(col.contact_offlimit, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Gated Talent] ' || nullif(REPLACE(cgt.contact_gatedtalent, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[URLs] ' || nullif(REPLACE(curl.contact_urls, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Salary] ' || nullif(REPLACE(px.salary::text, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Package] ' || nullif(REPLACE(px.package, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Previous candidate] ' || nullif(REPLACE(pc.value, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Relocate] ' || nullif(REPLACE(r.value, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Relocate locations] ' || nullif(REPLACE(jrll.relocate_location_list, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Preferred employment type] ' || nullif(REPLACE(pet.value, '\x0d\x0a', ' '), ''), NULL)		
	--	, coalesce('[DOB (estimated)] ' || nullif(case when p.isdateofbirthestimated = '1' then 'YES' else 'NO' end, ''), NULL)
	--	, coalesce('[Employment from] ' || nullif(REPLACE(cp.employmentfrom, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Employment to] ' || nullif(REPLACE(cp.employmentto, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Salary] ' || nullif(REPLACE(px.salary, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Employment assistant] ' || nullif(REPLACE(cp.employmentassistant, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Contact note] ' || chr(10) || nullif(REPLACE(px.note, '\x0d\x0a', ' '), ''), NULL)
	--	, coalesce('[Contact comment] ' || chr(10) || nullif(REPLACE(px.personcomment, '\x0d\x0a', ' '), ''), NULL)
	--) note
	, cd.candidate_document "contact-documents"
	FROM cte_contact cp
	JOIN (select * from personx where isdeleted = '0') px ON px.idperson = cp.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
	LEFT JOIN dup on dup.idperson = cp.idperson
	--LEFT JOIN personstatus ps ON ps.idpersonstatus = px.idpersonstatus_string
	LEFT JOIN selected_user u ON u.iduser = px.iduser
	--LEFT JOIN contact_offlimit col on col.idperson = cp.idperson
	LEFT JOIN country co ON co.idcountry = px.addressdefaultidcountry_string
	LEFT JOIN "location" l ON l.idlocation = px.idlocation_string
	--LEFT JOIN previouscandidate pc ON pc.idpreviouscandidate = px.idpreviouscandidate_string
	--LEFT JOIN relocate r ON px.idrelocate_string = r.idrelocate
	--LEFT JOIN cte_join_relocate_location_list jrll ON px.idperson = jrll.idperson
	--LEFT JOIN preferredemploymenttype pet ON px.idpreferredemploymenttype_string = pet.idpreferredemploymenttype
	LEFT JOIN title t on t.idtitle = p.idtitle
	--LEFT JOIN contact_gatedtalent cgt on cgt.idperson = cp.idperson
	--LEFT JOIN contact_urls curl on curl.idperson = cp.idperson
	LEFT JOIN candidate_document cd on cd.idperson = cp.idperson
	where cp.rn = 1
	and p.personid::bigint between 1 and 300000 --87559
	--and p.personid::bigint between 300000 and 1100000 --99958
	--and p.personid::bigint between 1100000 and 1200000 --99988
	--and p.personid::bigint between 1200000 and 1300000 --99994
	--and p.personid::bigint between 1300000 and 1400000 --99987
	--and p.personid::bigint between 1400000 and 2000000 --42865
	--and p.personid::bigint between 2000000 and 2091358 --91209