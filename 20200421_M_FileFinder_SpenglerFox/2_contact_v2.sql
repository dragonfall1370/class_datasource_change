WITH dup as (select idperson, TRIM('' FROM (TRIM('''' FROM translate(emailwork, ':?!\/#$%^&*()<>{}[]', '')))) as emailwork
	--distinct email if emails exist more than once
	, row_number() over(partition by trim(' ' from lower(translate(emailwork, ':?!\/#$%^&*()<>{}[]', ''))) order by idperson asc) as rn
	--distinct if contacts may have more than 1 email
	, row_number() over(partition by idperson order by trim(' ' from emailwork)) as contactrn
	from personx
	where emailwork like '%_@_%.__%')

--Relocation
, split_relocate_location_list AS (
	SELECT idperson
	, s.relocate_location
	FROM personx px, UNNEST(string_to_array(px.idrelocatelocation_string_list, ',')) s(relocate_location)
)

, relocate_location AS (
	SELECT idperson
	, l.value relocate_location
	FROM split_relocate_location_list srll
	LEFT JOIN "location" l ON srll.relocate_location = l.idlocation
)

, cte_join_relocate_location_list AS (
	SELECT idperson 
	, string_agg(relocate_location, ', ') relocate_location_list
	FROM relocate_location 
	GROUP BY idperson
)

--Offlimits
, contact_offlimit as (select a.idperson
	, string_agg(distinct
		concat_ws(chr(10)
			, coalesce('[Off limits] ' || nullif(case when a.isactive = '1' then 'YES' else 'NO' end, ''), NULL)
			, coalesce('[Off limit type] ' || nullif(REPLACE(olt.value, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Off limit by] ' || nullif(a.offlimitby, ''), NULL)
			, coalesce('[Off limit date from] ' || nullif(a.offlimitdatefrom, ''), NULL)
			, coalesce('[Off limit date to] ' || nullif(a.offlimitdateto, ''), NULL)
			, coalesce('[Off limit note] ' || chr(10) || nullif(REPLACE(a.offlimitnote, '\x0d\x0a', ' '), ''), NULL)
			, chr(10))
		, chr(13)) as contact_offlimit
	from personofflimit a
	left join offlimittype olt on olt.idofflimittype = a.idofflimittype
	--and a.idcompany = '603ca21b-b343-4869-abc0-4b0a2e2d707b'
	group by a.idperson)

--Linkedin
, linkedin as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '22cfe759-44fb-4378-9c23-16b19fa00935' --linkedin
	and e.commvalue is not NULL
	)
--GatedTalent
, gatedtalent as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '6b3fd179-fb26-4c9f-a22e-6eed1cc03aae' --GatedTalent
	and e.commvalue is not NULL
	)
, contact_gatedtalent as (select idperson
	, string_agg(commvalue, ', ') as contact_gatedtalent
	from gatedtalent
	group by idperson)
--URLs
, urls as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '46d6a515-b817-402b-82c3-62ac506854c0' --URL
	and e.commvalue is not NULL
	
	UNION
	select pe.idperson, pe.idpersoncommunicationtype, e.commvalue
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '8c7d16c4-125f-498b-b932-5465373a782b' --URL/URL
	and e.commvalue is not NULL
	)
, contact_urls as (select idperson
	, string_agg(commvalue, ', ') as contact_urls
	from urls
	group by idperson
	)
--Remuneration
, contact_remuneration as (select r.idremuneration, r.idcompany_person
		, r.bonus
		, rb.benefitnote
		, rb.benefitvalue
		, b.value as benefit
	from remuneration r
	left join remunerationbenefit rb on rb.idremuneration = r.idremuneration
	left join benefit b on b.idbenefit = rb.idbenefit
	where r.isdefault = '1'
	and coalesce(r.bonus, rb.benefitnote, rb.benefitvalue, b.value) is not NULL)

, contact_benefit as (select idcompany_person, idremuneration
		, concat_ws(chr(10)
			, coalesce('[Bonus] ' || nullif(replace(bonus, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Benefit note] ' || nullif(replace(benefitnote, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Benefit value] ' || nullif(replace(benefitvalue, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Benefit type] ' || nullif(replace(benefit, '\x0d\x0a', ' '), ''), NULL)
			) as contact_benefit
	from contact_remuneration)
	
--Contractors
, contact_contractor as (select c.idcontractor, c.idperson
		, c.minimumrequiredrate
		, c.idcurrency
		, cur.value
		, cur.currencyname
		, c.contractoravailabilitycomment
		, c.contractorpaymentcomment
		, concat_ws(chr(10)
			, coalesce('[Minimum required rate] ' || nullif(REPLACE(c.minimumrequiredrate, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Currency name] ' || nullif(REPLACE(cur.currencyname, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Contractor availability comment]' || chr(10) || nullif(REPLACE(c.contractoravailabilitycomment, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Contractor payment comment] ' || chr(10) || nullif(REPLACE(c.contractorpaymentcomment, '\x0d\x0a', ' '), ''), NULL)
			) as contact_contractor
		from contractor c
		left join currency cur on cur.idcurrency = c.idcurrency --value | currencyname
		where coalesce(c.minimumrequiredrate, c.idcurrency, c.contractoravailabilitycomment, c.contractorpaymentcomment) is not NULL
		)

--Documents
, documents as (select c.idcandidate, c.idperson
		, d.newdocumentname
		, reverse(substring(reverse(d.newdocumentname), 1, position('.' in reverse(d.newdocumentname)))) as extension
		, d.originaldocumentname
		, d.createdon::timestamp as created
		from personx c
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
, cte_contact AS (
	SELECT cp.idperson contact_id
	, ROW_NUMBER() OVER(PARTITION BY cp.idperson 
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
	, cp.sortorder
	, case when cp.idcompany in (select idcompany from company) then cp.idcompany
		else '999999999' end as company_id
	, TRIM(REPLACE(px.firstname, '\x0d\x0a', ' ')) first_name
	, TRIM(REPLACE(px.middlename, '\x0d\x0a', ' ')) middle_name
	, coalesce(nullif(TRIM(px.lastname), ''), 'Last name ' || cp.idperson) last_name
	, REPLACE(px.knownas, '\x0d\x0a', ' ') preferred_name
	, coalesce(nullif(directlinephone,''), nullif(px.defaultphone,'')) phone
	, concat_ws(',', nullif(mobileprivate,''), nullif(px.mobilebusiness,'')) mobile --#inject
	, case when dup.rn <> 1 then dup.rn || '_' || dup.emailwork 
		else nullif(dup.emailwork, '') end primary_email
	, TRIM('' FROM (TRIM('''' from translate(px.emailprivate, ':?!\/#$%^&*()<>{}[]', '')))) personal_email --#Inject
	, px.jobtitle title
	, u.useremail owner_email
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
	, cd.candidate_document
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON px.idperson = cp.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
	LEFT JOIN dup on dup.idperson = cp.idperson
	LEFT JOIN personstatus ps ON ps.idpersonstatus = px.idpersonstatus_string
	LEFT JOIN selected_user u ON u.iduser = px.iduser
	LEFT JOIN contact_offlimit col on col.idperson = cp.idperson
	LEFT JOIN country co ON co.idcountry = px.addressdefaultidcountry_string
	LEFT JOIN "location" l ON l.idlocation = px.idlocation_string
	LEFT JOIN previouscandidate pc ON pc.idpreviouscandidate = px.idpreviouscandidate_string
	LEFT JOIN relocate r ON px.idrelocate_string = r.idrelocate
	LEFT JOIN cte_join_relocate_location_list jrll ON px.idperson = jrll.idperson
	LEFT JOIN preferredemploymenttype pet ON px.idpreferredemploymenttype_string = pet.idpreferredemploymenttype
	LEFT JOIN title t on t.idtitle = p.idtitle
	LEFT JOIN contact_gatedtalent cgt on cgt.idperson = cp.idperson
	LEFT JOIN contact_urls curl on curl.idperson = cp.idperson
	LEFT JOIN candidate_document cd on cd.idperson = cp.idperson
	--where p.personid::bigint between 1 and 300000 --87559
	--where p.personid::bigint between 300000 and 1100000 --99958
	--where p.personid::bigint between 1100000 and 1200000 --99988
	--where p.personid::bigint between 1200000 and 1300000 --99994
	--where p.personid::bigint between 1300000 and 1400000 --99987
	--where p.personid::bigint between 1400000 and 2000000 --42865
	--where p.personid::bigint between 2000000 and 2091358 --91209
)

SELECT
	contact_id "contact-externalId"
	, COALESCE(company_id, '999999999') "contact-companyId"
	, first_name "contact-firstName"
	, middle_name "contact-middleName"
	, last_name "contact-lastName"
	, phone "contact-phone"
	, primary_email "contact-email"
	, personal_email
	, title "contact-jobTitle"
	, owner_email "contact-owners"
	, note "contact-note"
	, candidate_document "contact-documents"
FROM cte_contact
WHERE rn = 1