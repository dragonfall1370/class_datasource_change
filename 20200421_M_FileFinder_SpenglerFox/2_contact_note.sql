--CONTACT NOTE
WITH dup as (select idperson, TRIM('' FROM (TRIM('''' FROM emailwork))) as emailwork
	--distinct email if emails exist more than once
	, row_number() over(partition by trim(' ' from lower(emailwork)) order by idperson asc) as rn
	--distinct if contacts may have more than 1 email
	, row_number() over(partition by idperson order by trim(' ' from emailwork)) as contactrn
	from personx
	where emailwork like '%_@_%.__%')

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
	, string_agg(
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
--Education
, edu as (select e.idperson
		, e.educationestablishment
		, e.educationsubject
		, e.idqualification
		, e.notes
		, e.educationfrom
		, e.educationto
		, e.checkedon
		, e.checkedby
		, e.createdon
		, concat_ws(chr(10)
					, coalesce('[Education from] ' || nullif(REPLACE(e.educationfrom, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Education to] ' || nullif(REPLACE(e.educationto, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[College / University] ' || nullif(REPLACE(e.educationestablishment, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Course] ' || nullif(REPLACE(e.educationsubject, '\x0d\x0a', ' '), ''), NULL) --Education subject
					, coalesce('[Qualification] ' || nullif(REPLACE(q.value, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Checked by] ' || nullif(REPLACE(e.checkedby, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Checked on] ' || nullif(REPLACE(e.checkedon, '\x0d\x0a', ' '), ''), NULL)
					, coalesce('[Notes]' || chr(10) || nullif(REPLACE(e.notes, '\x0d\x0a', ' '), ''), NULL)
					) as edu
		from education e
		left join qualification q on e.idqualification = q.idqualification)

, contact_edu as (select idperson
		, string_agg(edu, chr(10) || chr(13) order by educationto desc, educationfrom desc, createdon asc) as contact_edu
		from edu
		group by idperson)

--Decision maker
, decisionmaker as (select pc.idperson
		, string_agg(u.value, ', ') as decisionmaker
		from personcode pc
		--left join tablemdshort ts on ts.idtablemd = pc.idtablemd --table name
		left join udskill3 u on u.idudskill3 = pc.codeid --Decision Maker
		where 1=1
		and pc.idtablemd = 'e81edcd2-7bf2-4e59-b24a-f9278f4f5c5e' --Decision Maker
		--and pc.idperson = '527db5ed-ee11-4412-be3d-cb069f153e31'
		group by pc.idperson)
		
--Xmas list
, xmaslist as (select pc.idperson
		, string_agg(u.value, ', ') as xmaslist
		from personcode pc
		--left join tablemdshort ts on ts.idtablemd = pc.idtablemd --table name
		left join udskill6 u on u.idudskill6 = pc.codeid --Xmas List
		where 1=1
		and pc.idtablemd = 'b801e205-4990-47d4-b237-46fe899da852' --Xmas List
		--and pc.idperson = '527db5ed-ee11-4412-be3d-cb069f153e31'
		group by pc.idperson)

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

--MAIN SCRIPT
, cte_contact AS (
	SELECT cp.idperson contact_id
	, case t.value
		when 'Miss' then 'Miss.'
		when 'Ms' then 'Ms.'
		when 'Dr' then 'Dr.'
		when 'Mr' then 'Mr.'
		when 'Mrs' then 'Mrs.'
		else NULL end gender_title --#inject
	, concat_ws(chr(10)
		, coalesce('[Contact ID] ' || nullif(REPLACE(px.idperson, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Contact status] ' || nullif(REPLACE(ps.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce(chr(10) || nullif(REPLACE(col.contact_offlimit, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Nationality] ' || nullif(REPLACE(px.nationalityvalue_string, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Gated Talent] ' || nullif(REPLACE(cgt.contact_gatedtalent, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[URLs] ' || nullif(REPLACE(curl.contact_urls, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Salary] ' || nullif(REPLACE(px.salary::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Package] ' || nullif(REPLACE(px.package, '\x0d\x0a', ' '), ''), NULL)
		, coalesce( chr(10) || nullif(REPLACE(cb.contact_benefit, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Previous candidate] ' || nullif(REPLACE(pc.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Person rating] ' || nullif(REPLACE(pr.value, '\x0d\x0a', ' '), ''), NULL) --contact rating	
		, coalesce('[Relocate] ' || nullif(REPLACE(r.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Relocate locations] ' || nullif(REPLACE(jrll.relocate_location_list, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Preferred employment type] ' || nullif(REPLACE(pet.value, '\x0d\x0a', ' '), ''), NULL)		
		, coalesce('[Gender] ' || nullif(REPLACE(g.value, '\x0d\x0a', ' '), ''), NULL) --p.idgender
		, coalesce('[DOB (estimated)] ' || nullif(case when p.isdateofbirthestimated = '1' then 'YES' else 'NO' end, ''), NULL)
		, coalesce('[Marital status] ' || nullif(REPLACE(ms.value, '\x0d\x0a', ' '), ''), NULL) --p.idmaritalstatus
		, coalesce('[Family] ' || nullif(REPLACE(px.family, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Modified by] ' || nullif(REPLACE(p.modifiedby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Modified on] ' || nullif(REPLACE(p.modifiedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Next available on] ' || nullif(REPLACE(px.nextavailableon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Minimum required rate] ' || nullif(REPLACE(px.minimumrequiredrate::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Unit type] ' || nullif(REPLACE(ut.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce(chr(10) || nullif(REPLACE(cct.contact_contractor, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Employment from] ' || nullif(REPLACE(cp.employmentfrom, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Employment to] ' || nullif(REPLACE(cp.employmentto, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Salary] ' || nullif(REPLACE(px.salary, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Employment assistant] ' || nullif(REPLACE(cp.employmentassistant, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Qualification] ' || nullif(REPLACE(px.qualificationvalue_string, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Education] ' || chr(10) || nullif(REPLACE(cedu.contact_edu, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[International] ' || nullif(REPLACE(px.internationalvalue_string, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Decision maker] ' || nullif(REPLACE(dm.decisionmaker, '\x0d\x0a', ' '), ''), NULL)	
		, coalesce('[Xmas list] ' || nullif(REPLACE(xl.xmaslist, '\x0d\x0a', ' '), ''), NULL)	
		, coalesce('[Contact note] ' || chr(10) || nullif(REPLACE(px.note, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Contact comment] ' || chr(10) || nullif(REPLACE(px.personcomment, '\x0d\x0a', ' '), ''), NULL)
----Additional info
		--, coalesce('[Maiden name] ' || nullif(REPLACE(px.maidenname, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Alert text] ' || nullif(REPLACE(px.alerttext, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Person relation type] ' || nullif(REPLACE(perrt.value, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Person relation created on] ' || nullif(REPLACE(perr.createdon, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Person relation created by] ' || nullif(REPLACE(perr.createdby, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Person relation description] ' || nullif(REPLACE(perr.relationdescription, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contact reference] ' || nullif(REPLACE(px.personreference, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Company name] ' || nullif(REPLACE(px.companyname, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Full default address] ' || nullif(REPLACE(px.addressdefaultfull, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Default country] ' || nullif(REPLACE(co.value, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Default postcode] ' || nullif(REPLACE(px.addressdefaultpostcode, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Default URL] ' || nullif(REPLACE(px.defaulturl, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Direct line phone] ' || nullif(REPLACE(px.directlinephone, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Business mobile] ' || nullif(REPLACE(px.mobilebusiness, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Other mobile] ' || nullif(REPLACE(px.mobileother, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Home phone] ' || nullif(REPLACE(px.phonehome, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Other phone] ' || nullif(REPLACE(px.phoneother, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Default Email] ' || nullif(REPLACE(px.defaultemail, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Other email] ' || nullif(REPLACE(px.emailother, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contractor unavailable reason] ' || nullif(REPLACE(cur.value, '\x0d\x0a', ' '), ''), NULL)	
		--, coalesce('[Location] ' || nullif(REPLACE(l.value, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contact origin] ' || nullif(REPLACE(po.value, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Market rate] ' || nullif(REPLACE(px.marketrate::text, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Owning user] ' || nullif(REPLACE(px.owninguser, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Previous company] ' || nullif(REPLACE(px.previouscompany, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Previous job title] ' || nullif(REPLACE(px.previousjobtitle, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contact subject] ' || chr(10) || nullif( REPLACE(p.contactsubject, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contacted by] ' || nullif(REPLACE(p.contactedby, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Contacted on] ' || nullif(REPLACE(p.contactedon, '\x0d\x0a', ' '), ''), NULL)
		--, coalesce('[Job note] ' || chr(10) || nullif(REPLACE(REPLACE(px.jobnotes, '\x0d\x0a', ' '), '\x0a', ' '), ''), NULL)
	--) note
	, cd.candidate_document
	FROM company_person cp
	JOIN (select * from personx where isdeleted = '0') px ON px.idperson = cp.idperson
	JOIN (select * from person where isdeleted = '0') p ON cp.idperson = p.idperson
	LEFT JOIN dup on dup.idperson = cp.idperson
	LEFT JOIN personstatus ps ON ps.idpersonstatus = px.idpersonstatus_string
	LEFT JOIN "user" u ON u.iduser = px.iduser
	LEFT JOIN contact_offlimit col on col.idperson = cp.idperson
	LEFT JOIN country co ON co.idcountry = px.addressdefaultidcountry_string
	LEFT JOIN "location" l ON l.idlocation = px.idlocation_string
	LEFT JOIN previouscandidate pc ON pc.idpreviouscandidate = px.idpreviouscandidate_string
	LEFT JOIN relocate r ON px.idrelocate_string = r.idrelocate
	LEFT JOIN cte_join_relocate_location_list jrll ON px.idperson = jrll.idperson
	LEFT JOIN unittype ut ON px.idunittype_string = ut.idunittype
	LEFT JOIN preferredemploymenttype pet ON px.idpreferredemploymenttype_string = pet.idpreferredemploymenttype
	LEFT JOIN personrating pr on pr.idpersonrating = px.idpersonrating_string
	LEFT JOIN gender g on g.idgender = p.idgender
	LEFT JOIN maritalstatus ms on ms.idmaritalstatus = p.idmaritalstatus
	LEFT JOIN contact_contractor cct on cct.idperson = cp.idperson
	LEFT JOIN title t on t.idtitle = p.idtitle
	LEFT JOIN contact_edu cedu on cedu.idperson = cp.idperson
	LEFT JOIN decisionmaker dm on dm.idperson = cp.idperson
	LEFT JOIN xmaslist xl on xl.idperson = cp.idperson
	LEFT JOIN contact_gatedtalent cgt on cgt.idperson = cp.idperson
	LEFT JOIN contact_urls curl on curl.idperson = cp.idperson
	LEFT JOIN contact_benefit cb on cb.idperson = cp.idperson
	LEFT JOIN candidate_document cd on cd.idperson = cp.idperson
----Additional joins	
	--LEFT JOIN contractorunavailablereason cur ON cur.idcontractorunavailablereason = px.idcontractorunavailablereason_string
	--LEFT JOIN personorigin po ON po.idpersonorigin = px.idpersonorigin_string
	--LEFT JOIN personrelation perr ON px.idperson = perr.idperson
	--LEFT JOIN personrelationtype perrt ON perr.idpersonrelationtype = perrt.idpersonrelationtype
)

SELECT
	contact_id con_ext_id
	, note "contact-note"
FROM cte_contact
WHERE rn = 1