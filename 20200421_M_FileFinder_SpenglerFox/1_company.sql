with dup as (SELECT idcompany, trim(replace(companyname, '\x0d\x0a', ' ')) as companyname
		, row_number() over(partition by trim(lower(replace(companyname, '\x0d\x0a', ' '))) order by idcompany asc) as rn 
		from company c
		where nullif(companyname, '') is not NULL)
		
, selected_user as (select iduser, idorganizationunit, title, firstname, lastname
		, fullname, replace(useremail, 'spenglerfox.eu', 'spenglerfox.com') as useremail, createdon
		from "user"
		where isdisabled = '0'
		and useremail ilike '%_@_%.__%')
		
, users as (select c.idcompany, c.iduser, u.useremail
		from company c
		left join selected_user u on u.iduser = c.iduser
		where c.iduser is not NULL
		
		UNION ALL
		
		select c.idcompany, c.iduser1, u.useremail
		from company c
		left join selected_user u on u.iduser = c.iduser1
		where c.iduser1 is not NULL
		
		UNION ALL
		
		select c.idcompany, c.iduser, u.useremail
		from companyassociate c
		left join selected_user u on u.iduser = c.iduser
		)
		
, company_user as (select idcompany
		, string_agg(distinct lower(useremail), ',') as company_user
		from users
		where useremail is not NULL
		group by idcompany)
	
--Location | Location name
, com_location as (select com.idcompany, com.idlocation
		, l."value" as location_address
		, l."value" as location_name
		, case when c.abbreviation = 'UK' then 'GB'
				when c.abbreviation = 'CD' then 'CG'
				when c.value in ('Unknown', '--') THEN NULL
				when c.value = 'United States' then 'US'
				when c.value = 'Russian Federation' then 'RU'
				when c.value = 'Ireland' then 'IE'
				when c.value = 'Serbia' then 'RS'
				when c.value = 'United States of America' THEN 'US'
				when c.value = 'United Kingdom' THEN 'GB'
				when c.value = 'Canada' THEN 'CA'
				when c.value = 'New Zealand' THEN 'NZ'
				when c.value = 'Czech Republic' THEN 'CZ'
				when c.value = 'Germany' THEN 'DE'
				when c.value = 'Philippines' THEN 'PH'
				when c.value = 'Spain' THEN 'ES'
				when c.value = 'Australia' THEN 'AU'
				else c.abbreviation end as location_country
		from company com
		left join "location" l on l.idlocation = com.idlocation
		left join country c on c.value = l.value
		where com.idlocation is not NULL)

--Research
, company_search as (select a.idcompany
	, string_agg(distinct
		concat_ws(chr(10)
			, coalesce('[Researched on] ' || nullif(a.researchedon, ''), NULL)
			, coalesce('[Researched by] ' || nullif(a.researchedby, ''), NULL)
			, coalesce('[Target note] ' || nullif(a.targetnote, ''), NULL)
			, coalesce('[Assignment title] ' || nullif(asm.assignmenttitle, ''), NULL)
			, chr(10))
		, chr(13)) as company_research
	from assignmenttarget a
	left join "assignment" asm on asm.idassignment = a.idassignment
	where coalesce(a.researchedon, a.researchedby) is not NULL
	--and a.idcompany = '603ca21b-b343-4869-abc0-4b0a2e2d707b'
	group by a.idcompany)
	
--Offlimits
, company_offlimit as (select a.idcompany
	, string_agg(distinct
		concat_ws(chr(10)
			, coalesce('[Off limits] ' || nullif(case when a.isactive = '1' then 'YES' else 'NO' end, ''), NULL)
			, coalesce('[Off limit by] ' || nullif(a.offlimitby, ''), NULL)
			, coalesce('[Off limit date from] ' || nullif(a.offlimitdatefrom, ''), NULL)
			, coalesce('[Off limit date to] ' || nullif(a.offlimitdateto, ''), NULL)
			, coalesce('[Off limit note] ' || nullif(a.offlimitnote, ''), NULL)
			, chr(10))
		, chr(13)) as company_offlimit
	from companyofflimit a
	--and a.idcompany = '603ca21b-b343-4869-abc0-4b0a2e2d707b'
	group by a.idcompany)

--Divisions
, com_division as (select cd.idcompany
		, string_agg(d.divisionname, ', ') as com_division
		from company_division cd
		left join division d on d.iddivision = cd.iddivision
		group by cd.idcompany)

--Documents
, documents as (select c.idcompany
		, d.newdocumentname
		, reverse(substring(reverse(d.newdocumentname), 1, position('.' in reverse(d.newdocumentname)))) as extension
		, d.originaldocumentname
		, d.createdon::timestamp as created
		from company c
		join entitydocument ed on ed.entityid = c.idcompany
		join document d on d.iddocument = ed.iddocument
		where ed.entityid is not NULL
		and d.newdocumentname is not NULL)
		
, company_document as (select idcompany
		, string_agg(newdocumentname, ',' order by created desc) as company_document
		from documents
		where extension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')
		group by idcompany)

--MAIN SCRIPT
select c.idcompany "company-externalId"
	, case when dup.rn <> 1 then concat(dup.companyname, ' - ', rn)
			else coalesce(nullif(replace(companyname, '\x0d\x0a', ' '), ''), 'No company name ' || c.idcompany) end as "company-name"
	, c.parentid --#inject
	, c.branch --location name
	, c.idlocation --location name
	--, cl.location_address "company-locationAddress"
	--, cl.location_name "company-locationName"
	--, cl.location_country "company-locationCountry"
	, coalesce(c.branch, REPLACE(cx.addressdefaultfull, '\x0d\x0a', ' ')) "company-locationName"
	, REPLACE(cx.addressdefaultfull, '\x0d\x0a', ' ') "company-locationAddress"
	, REPLACE(cx.addressdefaultcity, '\x0d\x0a', ' ') "company-locationCity"
	, REPLACE(cx.addressdefaultpostcode, '\x0d\x0a', ' ') "company-locationZipCode"
	, REPLACE(cx.addressdefaultcountystate, '\x0d\x0a', ' ') "company-locationState"
	, case when co.abbreviation = 'UK' then 'GB'
				when co.abbreviation = 'CD' then 'CG'
				when co.abbreviation in ('Unknown', '--') THEN NULL
				when co.value in ('Unknown', '--') THEN NULL
				when co.value = 'United States' then 'US'
				when co.value = 'Russian Federation' then 'RU'
				when co.value = 'Ireland' then 'IE'
				when co.value = 'Serbia' then 'RS'
				when co.value = 'United States of America' THEN 'US'
				when co.value = 'United Kingdom' THEN 'GB'
				when co.value = 'Canada' THEN 'CA'
				when co.value = 'New Zealand' THEN 'NZ'
				when co.value = 'Czech Republic' THEN 'CZ'
				when co.value = 'Germany' THEN 'DE'
				when co.value = 'Philippines' THEN 'PH'
				when co.value = 'Spain' THEN 'ES'
				when co.value = 'Australia' THEN 'AU'
				else co.abbreviation end "company-locationCountry"
	, cu.company_user as "company-owners"
	, cx.companyswitchboard as "company-phone"
	, c.noofemployees --#inject
	, left(cx.defaulturl,100) as "company-website"
	, concat_ws(chr(10)
			, coalesce('[Company External ID] ' || nullif(REPLACE(c.idcompany, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Company ID] ' || nullif(REPLACE(c.companyid, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Default company email] ' || nullif(REPLACE(cx.defaultemail, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Company status] ' || nullif(REPLACE(cs.value, '\x0d\x0a', ' '), ''), NULL) --company status
			, coalesce('[Researched on] ' || nullif(REPLACE(cx.researchedon, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Researched by] ' || nullif(REPLACE(cx.researchedby, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Off limits]' || chr(10) || nullif(REPLACE(co.company_offlimit, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[New client date] ' || nullif(REPLACE(ce.udfield1, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Division] ' || nullif(REPLACE(cdiv.com_division, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Group name] ' || nullif(REPLACE(cx.groupname, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Company location] ' || nullif(REPLACE(cl.location_name, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Company note]' || chr(10) || nullif(REPLACE(cx.companynote,  '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Company comment]' || chr(10) || nullif(REPLACE(cx.companycomment, '\x0d\x0a', ' '), ''), NULL)
			) as "company-note"
	, cd.company_document "company-document"
from company c
left join dup on dup.idcompany = c.idcompany
left join companystatus cs on cs.idcompanystatus = c.idcompanystatus --cs.value
left join companyx cx on cx.idcompany = c.idcompany
left join companyext ce on ce.idcompany = c.idcompany
left join com_division cdiv on cdiv.idcompany = c.idcompany
left join company_document cd on cd.idcompany = c.idcompany
left join com_location cl on cl.idcompany = c.idcompany
left join company_user cu on cu.idcompany = c.idcompany --owners
left join company_offlimit co on co.idcompany = c.idcompany
left join country co on co.idcountry = cx.addressdefaultidcountry_string
--where c.idcompany = 'd7b59690-a8ff-4f5f-83f6-f4836fd336d1'

UNION ALL

select '999999999', 'Default company', '', '', '', '', '', '', '', '', '', '', '', '', '', 'This is default company from data migration', NULL