with consultant as (select f.uniqueid, "105 perm cons xref"
	, f17."1 name alphanumeric" as consultant
	, f17."72 email add alphanumeric" as email
	from f01 f
	left join f17 on f17.uniqueid = f."105 perm cons xref"
	where "105 perm cons xref" is not NULL
	
	UNION
	select f.uniqueid, "5 temp cons xref"
	, f17."1 name alphanumeric"
	, f17."72 email add alphanumeric"
	from f01 f
	left join f17 on f17.uniqueid = f."5 temp cons xref"
	where "5 temp cons xref" is not NULL)
	
, con_owners as (select uniqueid
	, string_agg(distinct email, ',') as com_owners
	from consultant
	group by uniqueid)

--EMAIL ADDRESS
, email as (select uniqueid
	, "4 ref no numeric" ref_id
	, "33 e-mail alphanumeric" org_email
	, a.email
	, case when position('--' in email) > 0 or position('?' in email) > 0 or email = '' then NULL
		else trim('''' from trim(email)) end as new_email
	, a.splitrn
	from f01, unnest(string_to_array("33 e-mail alphanumeric", '~')) with ordinality as a(email, splitrn)
	where "33 e-mail alphanumeric" ilike '%_@_%.__%')
	
, person_email as (select uniqueid, ref_id, org_email, new_email, splitrn
	, row_number() over(partition by new_email order by ref_id) as rn --distinct email if more than once
	, row_number() over(partition by uniqueid order by splitrn, new_email desc) as person_rn --distinct if contacts may have more than 1 email
	from email
	where new_email is not NULL
	) --select * from person_email

--PHONE ADDRESS INCLUDING ALL H-W-M PHONES
, phone as (select uniqueid
	, "38 ph hwo alphanumeric" as org_phone
	, regexp_replace(regexp_replace(regexp_replace(split_part("38 ph hwo alphanumeric", '~', 1), '[^0-9]+ - ', ''), '[^\w -/]', '', 'g'), '[[:alpha:]]', '', 'g') as homephone
	, regexp_replace(regexp_replace(regexp_replace(split_part("38 ph hwo alphanumeric", '~', 2), '[^0-9]+ - ', ''), '[^\w -/]', '', 'g'), '[[:alpha:]]', '', 'g') as workphone
	, regexp_replace(regexp_replace(regexp_replace(split_part("38 ph hwo alphanumeric", '~', 3), '[^0-9]+ - ', ''), '[^\w -/]', '', 'g'), '[[:alpha:]]', '', 'g') as mobile
	from f01
	where nullif("38 ph hwo alphanumeric", '') is not NULL
	)
	
--DOCUMENT
, person_document as (select uniqueid
	, string_agg(right("relative document path", position(E'\\' in reverse("relative document path")) - 1), ',') as person_document
	from f01docs1
	group by uniqueid
	)

--EMPLOYMENT WORK HISTORY
, work_history as (select f04.uniqueid
	, "1 candidate xref" as person_id
	, "4 from date"
	, to_date(nullif(regexp_replace(regexp_replace("4 from date", '[^0-9]+/', ''), '[[:alpha:]]+\W', '', 'g'), ' '), 'DD/MM/YY') as from_date --due to invalid date
	, "5 to date"
	, to_date(nullif(regexp_replace(regexp_replace("5 to date", '[^0-9]+/', ''), '[[:alpha:]]+\W', '', 'g'), ' '), 'DD/MM/YY') as to_date --due to invalid date
	, "2 company xref"
	, case when "2 company xref" = f02.uniqueid then f02."1 name alphanumeric"
			else "2 company xref" end as employer
	, "6 position alphanumeric" as job_title
	, row_number() over(partition by "1 candidate xref"
				order by to_date("5 to date", 'DD/MM/YY') desc, to_date("4 from date", 'DD/MM/YY') desc
				, case "20 status codegroup   7"
						when 'C' then 1
						when 'F' then 2
						when 'P' then 3
						when 'U' then 4 end asc) as rn
	, "13 type codegroup  46"
	, c46.description as emp_type
	, "20 status codegroup   7"
	, c7.description as emp_status
	from f04
	left join f02 on f02.uniqueid = f04."2 company xref" --get company name if using uniqueid
	left join (select * from codes where codegroup = '46') c46 on c46.code = f04."13 type codegroup  46"
	left join (select * from codes where codegroup = '7') c7 on c7.code = f04."20 status codegroup   7"
	where 1=1
	--and ("4 from date" ilike '%An%' or "5 to date" ilike '%An%')
	--and "1 candidate xref" = '80810101A1848080'
	)
	
, person_wh as (select *
	, row_number() over(partition by person_id
											order by to_date desc, from_date desc
										, case "20 status codegroup   7"
											when 'C' then 1
											when 'F' then 2
											when 'P' then 3
											when 'U' then 4 end asc) as rn
	from work_history
	where employer is not NULL and job_title is not NULL
	) --select * from person_wh
	
, wh_summary as (select person_id
	, string_agg(concat_ws(chr(10)
			, ('From date: ' || from_date), ('To date: ' || to_date), ('Employer: ' || employer), ('Job title: ' || job_title)
			, ('Type: ' || emp_type), ('Status: ' || emp_status))
			, chr(10) || chr(13) order by rn) as person_wh
	from work_history
	group by person_id)

--MAIN SCRIPT
select c.uniqueid as "contact-externalId"
, case when c."16 employer xref" not in (select uniqueid from f02) or nullif(c."16 employer xref", '') is NULL then 'MM999999999'
	else c."16 employer xref" end as "contact-companyId"
, c."4 ref no numeric"
, c."185 surname alphanumeric" as first_name
, c."186 forenames alphanumeric" as last_name
, case when pe.rn > 1 then pe.rn || '_' || pe.new_email
		else pe.new_email end as "contact-email"
, p.workphone as "contact-phone"
, p.mobile as "contact-mobile" --#inject
, case 
		when c."27 title codegroup  16" = 'ENG' then NULL
		when c."27 title codegroup  16" = 'MIS' then 'MISS'
		else c."27 title codegroup  16" end as "contact-title" --salutation
, c."96 cont posn alphanumeric" as "contact-jobTitle"
, c."109 cont addr alphanumeric" as current_location --#inject
, c."110 cont pstcd alphanumeric" as current_postcode --#inject
, pd.person_document as "contact-document"
, concat_ws(chr(10)
	, coalesce('Reference number: ' || c."4 ref no numeric", NULL)
	, coalesce('Created: ' || c."28 created date", NULL)
	, coalesce('Created by: ' || f17."1 name alphanumeric" || ' - ' || f17."72 email add alphanumeric", NULL)
	, coalesce('Updated: ' || c."29 updated date", NULL)
	, coalesce('Updated by: ' || u2."1 name alphanumeric" || ' - ' || u2."72 email add alphanumeric", NULL)
	, coalesce('Last contact date: ' || c."41 last cont date", NULL)
	) as "contact-note"
from f01 c
left join (select * from person_email where person_rn=1) pe on pe.uniqueid = c.uniqueid --get only 1 email for contact
left join phone p on p.uniqueid = c.uniqueid --all phone
left join person_document pd on pd.uniqueid = c.uniqueid
left join f17 on f17.uniqueid = c."62 created by xref"
left join f17 u2 on u2.uniqueid = c."37 updated by xref"
where c."100 contact codegroup  23" = 'Y'
--and "1 name alphanumeric" ilike '%Eduardo%Cano%' --test contact