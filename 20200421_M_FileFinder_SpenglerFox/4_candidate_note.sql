WITH 
split_relocate_location_list AS (
	SELECT 
	idperson, 
	s.relocate_location
	FROM personx px, UNNEST(string_to_array(px.idrelocatelocation_string_list, ',')) s(relocate_location)
)
, relocate_location AS (
	SELECT
	idperson,
	l.value relocate_location
	FROM split_relocate_location_list srll
	LEFT JOIN "location" l ON srll.relocate_location = l.idlocation
)
, cte_join_relocate_location_list AS (
	SELECT 
	idperson, 
	string_agg(relocate_location, ', ') relocate_location_list
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

, contact_benefit as (select cp.idperson, cr.idcompany_person, cr.idremuneration
		, concat_ws(chr(10)
			, coalesce('[Bonus] ' || nullif(replace(cr.bonus, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Benefit note] ' || nullif(replace(cr.benefitnote, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Benefit value] ' || nullif(replace(cr.benefitvalue, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Benefit type] ' || nullif(replace(cr.benefit, '\x0d\x0a', ' '), ''), NULL)
			) as contact_benefit
	from contact_remuneration cr
	join company_person cp on cp.idcompany_person = cr.idcompany_person)

--Linkedin | not having multiple linkedin
, linkedin as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as linkedin
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '22cfe759-44fb-4378-9c23-16b19fa00935' --linkedin
	and e.commvalue is not NULL
	)
--GatedTalent | not having multiple GatedTalent
, gatedtalent as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as gatedtalent
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '6b3fd179-fb26-4c9f-a22e-6eed1cc03aae' --GatedTalent
	and e.commvalue is not NULL
	)
--Company URL
, companyurl as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as companyurl
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = 'd3322ae4-01cf-4302-ac36-6e52504bcb77' --URL Company
	and e.commvalue is not NULL
	)
--Candidate URLs (may include Linkedin)
, candidateurl as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as candidateurl
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '8c7d16c4-125f-498b-b932-5465373a782b' --URLs
	and e.commvalue is not NULL
	)
--Switchboard | not having multiple switchboard
, candidate_switchboard as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as candidate_switchboard
		from person_eaddress pe
		left join eaddress e on e.ideaddress = pe.ideaddress
		where pe.idpersoncommunicationtype = 'dced2973-8162-4152-a75a-a7d7991d1577' --switchboard
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
--Candidate work history
, company_cand as (select *
		, ROW_NUMBER() OVER(PARTITION BY cp.idperson 
				ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
		from company_person cp)

, work_history as (select px.idperson, px.idcompany, cp.idcompany, px.companyname
		, px.jobtitle, px.fromdate, cp.employmentfrom, px.todate, cp.employmentto
		, px.idpersonstatus_string, px.salary, cp.salary, cp.checkedon, cp.checkedby, cp.employmentassistant, cp.sortorder
		from personx px
		left join (select * from company_cand where rn = 1) cp on cp.idperson = px.idperson
		left join personstatus ps on ps.idpersonstatus = px.idpersonstatus_string
		where px.isdeleted = '0'
		--and px.idperson = 'cd2260b2-80b9-42e6-b2ee-4a5c542c87d8'
		--order by px.idperson
		)
--MAIN SCRIPT
SELECT c.idperson cand_ext_id
	, concat_ws('<br/>'
		, coalesce('[Candidate External ID] ' || nullif(REPLACE(px.idperson, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Candidate ID] ' || nullif(REPLACE(px.personid::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Candidate status] ' || nullif(REPLACE(ps.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Salutation] ' || nullif(REPLACE(px.salutation, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('<br/>' || nullif(REPLACE(col.contact_offlimit, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Nationality] ' || nullif(REPLACE(px.nationalityvalue_string, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Gated talent] ' || nullif(REPLACE(gt.gatedtalent, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Company URL] ' || nullif(REPLACE(comu.companyurl, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Candidate URL] ' || nullif(REPLACE(canu.candidateurl, '\x0d\x0a', ' '), ''), NULL) --#inject
		, coalesce('[Salary] ' || nullif(REPLACE(px.salary::text, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Package] ' || nullif(REPLACE(px.package, '\x0d\x0a', ' '), ''), NULL)
		, coalesce( chr(10) || nullif(REPLACE(cb.contact_benefit, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Location] ' || nullif(REPLACE(l.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Previous candidate] ' || nullif(REPLACE(pc.value, '\x0d\x0a', ' '), ''), NULL) --px.idpreviouscandidate_string
		, coalesce('[Person rating] ' || nullif(REPLACE(prat.value, '\x0d\x0a', ' '), ''), NULL) --px.idpersonratingstring
		, coalesce('[Preferred employment type] ' || nullif(REPLACE(pet.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Candidate note] ' || '<br/>' || nullif(REPLACE(px.note, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Candidate comment] ' || '<br/>' || nullif(REPLACE(px.personcomment, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Biography] ' || '<br/>' || nullif(REPLACE(px.biography, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Relocation] ' || nullif(REPLACE(reloc.relocate_location_list, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Is DOB estimated] ' || '<br/>' || nullif(REPLACE(case when px.isdateofbirthestimated = '1' then 'YES' else 'NO' end, '\x0d\x0a', ' '), ''), NULL)	
		, coalesce('[Family] ' || nullif(REPLACE(px.family, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Unavailable reason] ' || '<br/>' || nullif(REPLACE(cur.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Modified on] ' || nullif(REPLACE(px.modifiedon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Modified by] ' || nullif(REPLACE(px.modifiedby, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Qualification] ' || nullif(REPLACE(px.qualificationvalue_string, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Next available on] ' || nullif(REPLACE(px.nextavailableon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Rate information on] ' || nullif(REPLACE(ctr.rateinformationon, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('<br/>' || nullif(REPLACE(cctr.contact_contractor, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Contractor company name] ' || nullif(REPLACE(ctr.contractorcompanyname, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Contractor company registration number] ' || nullif(REPLACE(ctr.contractorcompanyregistrationnumber, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Processing reason] ' || nullif(REPLACE(pr.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Processing status] ' || nullif(REPLACE(pst.value, '\x0d\x0a', ' '), ''), NULL)
		, coalesce('[Origin of data] ' || nullif(REPLACE(px.originofdata, '\x0d\x0a', ' '), ''), NULL)
	) note
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
	LEFT JOIN personstatus ps ON ps.idpersonstatus = px.idpersonstatus_string
	LEFT JOIN contact_offlimit col on col.idperson = p.idperson
	LEFT JOIN linkedin lk on lk.idperson = p.idperson
	LEFT JOIN candidate_switchboard cw on cw.idperson = p.idperson
	LEFT JOIN gatedtalent gt on gt.idperson = p.idperson
	LEFT JOIN companyurl comu on comu.idperson = p.idperson
	LEFT JOIN candidateurl canu on canu.idperson = p.idperson
	LEFT JOIN previouscandidate pc ON pc.idpreviouscandidate = px.idpreviouscandidate_string
	LEFT JOIN personrating prat ON prat.idpersonrating = px.idpersonrating_string
	LEFT JOIN preferredemploymenttype pet ON pet.idpreferredemploymenttype = px.idpreferredemploymenttype_string
	LEFT JOIN maritalstatus mrs on mrs.idmaritalstatus = px.idmaritalstatus_string
	LEFT JOIN contractorunavailablereason cur ON cur.idcontractorunavailablereason = px.idcontractorunavailablereason_string
	LEFT JOIN countystate cs ON px.addressdefaultcountystate = cs.abbreviation
	LEFT JOIN (select * from contractor where idcurrency is not NULL) ctr on ctr.idperson = p.idperson
	LEFT JOIN title t on t.idtitle = px.idtitle_string
	LEFT JOIN contact_contractor cctr on cctr.idperson = p.idperson
	LEFT JOIN processingreason pr ON pr.idprocessingreason = px.idprocessingreason_string
	LEFT JOIN processingstatus pst ON pst.idprocessingstatus = px.idprocessingstatus_string
	LEFT JOIN "location" l ON l.idlocation = px.idlocation_string
	LEFT JOIN cte_join_relocate_location_list reloc on reloc.idperson = p.idperson
	LEFT JOIN contact_benefit cb on cb.idperson = c.idperson