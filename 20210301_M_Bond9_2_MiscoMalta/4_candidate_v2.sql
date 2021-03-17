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
	, string_agg(distinct email, ',') as con_owners
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
	, row_number() over(partition by uniqueid order by splitrn, new_email desc) as person_rn --distinct if candidates may have more than 1 email
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
	
--DOCUMENT | CHANGED TO NEW APPROACH
, person_document as (select uniqueid
	, string_agg(right("relative document path", position(E'\\' in reverse("relative document path")) - 1), ',') as person_document
	from f01docs1_edited
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
	, "10 salary numeric" as salary
	from f04
	left join f02 on f02.uniqueid = f04."2 company xref" --get company name if using uniqueid
	left join (select * from codes where codegroup = '46') c46 on c46.code = f04."13 type codegroup  46"
	left join (select * from codes where codegroup = '7') c7 on c7.code = f04."20 status codegroup   7"
	where 1=1
	--and "4 from date" ilike '%ma%'
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
											when 'U' then 4 end asc) as person_wh_rn
	from work_history
	where employer is not NULL and job_title is not NULL
	) --select * from person_wh where person_id = '8081010180968080'
	
, wh_summary as (select person_id
	, string_agg(concat_ws('<br/>'
			, ('From date: ' || from_date), ('To date: ' || to_date), ('Employer: ' || employer), ('Job title: ' || job_title), ('Salary: ' || salary)
			, ('Type: ' || emp_type), ('Status: ' || emp_status))
			, '<br/>' order by rn) as person_wh
	from work_history
	group by person_id)

, nationality as (select uniqueid, "63 nationalit codegroup  14"
	, c14.description
	, c.countrycode
	from f01
	left join (select * from codes where codegroup = '14') c14 on c14.code = f01."63 nationalit codegroup  14"
	left join country_code_nationality c on c.nationality = c14.description --TEMP TABLE FOR COUNTRY LIBRARY
	)

--ADDRESS
, address as (select uniqueid
	, "100 contact codegroup  23"
	, "101 candidate codegroup  23"
	, "109 cont addr alphanumeric" as address
	, split_part("109 cont addr alphanumeric", '~', 3) as city
	, split_part("109 cont addr alphanumeric", '~', 5) as country
	, "110 cont pstcd alphanumeric" as postcode
	from f01
	where nullif("109 cont addr alphanumeric", '') is not NULL
	)

--SKILL LIST
, skill_list as (select f01.uniqueid, "9 skills xref"
	, skill
	, splitrn
	, f12."1 attribute alphanumeric" as skill_list
	from f01, unnest(string_to_array("9 skills xref", '~')) with ordinality as a(skill, splitrn)
	left join f12 on f12.uniqueid = a.skill
	where "9 skills xref" is not NULL
	)
	
, person_skill as (select uniqueid
	, string_agg(skill_list, ', ') as person_skill
	from skill_list
	group by uniqueid
	) --select * from person_skill
	
--EDUCATION | TEMP TABLE
, edu_summary as (select uniqueid
	, string_agg(concat_ws(chr(10)
			, ('Highest Education level: ' || edu_lvl)
			, ('Date: ' || edu_date), ('To date: ' || edu_to), ('Subject: ' || edu_text), ('Educations details: ' || edu_qualification)
			, ('Grade: ' || edu_grade), ('Professional Qualifications: ' || edu_prod_qualif), ('Study status: ') || study_stat)
			, chr(10) || chr(13) order by arn) as person_edu_summary
	from education
	group by uniqueid
	)
	
--MAIN SCRIPT
select c.uniqueid as "candidate-externalId"
, c."4 ref no numeric"
, c."26 salutation alphanumeric" as nick_name --#inject
, coalesce(c."185 surname alphanumeric", c."26 salutation alphanumeric", 'No lastname') as "candidate-lastName" --changed on 20201126
, coalesce(c."186 forenames alphanumeric", c."26 salutation alphanumeric", 'No firstname -' || c."4 ref no numeric") as "candidate-firstName" --changed on 20201126
--CAND INFO
, case when pe.rn > 1 then pe.rn || '_' || pe.new_email
		else coalesce(pe.new_email, c."4 ref no numeric" || '_candidate@noemail.com') end as "candidate-email"
, p.mobile as "candidate-mobile" --#inject
, p.mobile as "candidate-phone"
, p.workphone as "candidate-workPhone"
, p.homephone as "candidate-homePhone"
, co.con_owners as "candidate-owners"
, c."118 web add alphanumeric" as "website" --#inject
, to_date(c."134 avail from date", 'DD/MM/YY') as "available_for_work" --#inject
, case 
		when c."27 title codegroup  16" in ('ENG', 'OR') then NULL
		when c."27 title codegroup  16" = 'MIS' then 'MISS'
		else c."27 title codegroup  16" end as "candidate-title" --salutation
, to_date("39 dob date", 'DD/MM/YY') as "candidate-dob"
, n.countrycode as "candidate-citizenship"
, case "198 gender codegroup  64"
		when 'M' then 'MALE'
		when 'F' then 'FEMALE'
		else NULL end as "candidate-gender"
--ADDITIONAL INFO
, c."221 idnum alphanumeric" as passport --#inject
--SALARY INFO
, 'EUR' as "candidate-currency"
, case when c."18 sal reqd numeric" ~ '^\d+(\.\d+)?$' then c."18 sal reqd numeric"::float else NULL end as "candidate-desiredSalary"
, case when c."20 rate reqd numeric" ~ '^\d+(\.\d+)?$' then c."20 rate reqd numeric"::float else NULL end as "candidate-contractRate"
--CONTACT LOCATION
--, "34 cand addr alphanumeric"
, replace(concat_ws(', '
	, coalesce(nullif(split_part("34 cand addr alphanumeric", '~', 1), ''), NULL)
	, coalesce(nullif(split_part("34 cand addr alphanumeric", '~', 2), ''), NULL)
	, coalesce(nullif(split_part("34 cand addr alphanumeric", '~', 3), ''), NULL)
	, coalesce(nullif(split_part("34 cand addr alphanumeric", '~', 4), ''), NULL)
	, coalesce(nullif(split_part("34 cand addr alphanumeric", '~', 5), ''), NULL)
	), ',,', '') as "candidate-address"
, split_part(c."34 cand addr alphanumeric", '~', 3) as "candidate-city"
, split_part(c."34 cand addr alphanumeric", '~', 4) as "candidate-state"
, case right(c."34 cand addr alphanumeric", position('~' in reverse(c."34 cand addr alphanumeric")) - 1)
		when 'New York, United States' then 'US'
		when 'Malta' then 'MT'
		when 'Cyprus' then 'CY'
		when 'US' then 'US'
		when 'Pakistan' then 'PK'
		when 'United States' then 'US'
		when 'Italy' then 'IT'
		when 'Isle Of Man' then 'IM'
		when 'India' then 'IN'
		when 'Switzerland' then 'CH'
		when 'Sweden' then 'SE'
		when 'Malta ' then 'MT'
		when 'Saudi Arabia' then 'SA'
		when 'United Kingdom' then 'GB'
		when 'Germany' then 'DE'
		when 'Hong Kong' then 'HK'
		when 'UK' then 'GB'
		when 'Great Britain' then 'GB'
		when 'UNITED KINGDOM' then 'GB'
		when 'NIGERIA ' then 'NG'
		when 'Gibraltar' then 'GI'
		when 'Ireland' then 'IE'
		when 'London' then 'GB'
		when 'MALTA' then 'MT'
		when 'Ukraine' then 'UA'
		when 'Usa' then 'US'
		when 'Italia' then 'IT'
		when 'Spain' then 'ES'
		when 'malta' then 'MT'
		when 'Qatar ' then 'QA'
		when 'Qatar ' then 'QA'
		when 'South Africa ' then 'ZA'
		when 'France' then 'FR'
		when 'United Arab Emirates' then 'AE'
		when 'Malaysia' then 'MY'
	else NULL end as "candidate-Country"
--CAND WORK HISTORY
--, f02."1 name alphanumeric" as "candidate-employer1"
--, c."96 cont posn alphanumeric" as "candidate-jobTitle1"
, pw.employer as "candidate-employer1"
, pw.job_title as "candidate-jobTitle1"
, pw.from_date as "candidate-startDate1"
, pw.to_date as "candidate-endDate1"
--, wh.person_wh as "candidate-workHistory" --inject manually
--EDUCATION
--, es.person_edu_summary as "candidate-education" --inject manually
, ps.person_skill as "candidate-skills"
, concat_ws('<br/>'
	, ('Reference number: ' || c."4 ref no numeric")
	, coalesce('Status: ' || nullif(c4.description, ''), NULL)
	, coalesce('Internal Interview date: ' || nullif(c."31 intvw date date", ''), NULL) --added 2021-02-24
	, coalesce('Created: ' || c."28 created date", NULL)
	, coalesce('Created by: ' || f17."1 name alphanumeric" || ' - ' || f17."72 email add alphanumeric", NULL)
	, coalesce('Updated: ' || c."29 updated date", NULL)
	, coalesce('Updated by: ' || u2."1 name alphanumeric" || ' - ' || u2."72 email add alphanumeric", NULL)
	, coalesce('Last contact date: ' || c."41 last cont date", NULL)
	) as "candidate-note"
--, pd.person_document as "candidate-document"
from f01 c
left join con_owners co on co.uniqueid = c.uniqueid
left join (select * from person_email where person_rn=1) pe on pe.uniqueid = c.uniqueid --get only 1 email for candidate
left join phone p on p.uniqueid = c.uniqueid --phone
--left join person_document pd on pd.uniqueid = c.uniqueid
left join nationality n on n.uniqueid = c.uniqueid --nationality
--left join address a on a.uniqueid = c.uniqueid --address
left join f02 on f02.uniqueid = c."16 employer xref" --company
left join person_skill ps on ps.uniqueid = c.uniqueid
left join wh_summary wh on wh.person_id = c.uniqueid
left join edu_summary es on es.uniqueid = c.uniqueid
left join (select * from person_wh where person_wh_rn = 1) pw on pw.person_id = c.uniqueid
left join (select * from codes where codegroup = '4') c4 on c4.code = c."3 status codegroup   4"
left join f17 on f17.uniqueid = c."62 created by xref"
left join f17 u2 on u2.uniqueid = c."37 updated by xref"
where 1=1
and c."101 candidate codegroup  23" = 'Y'
--and "1 name alphanumeric" ilike '%Eduardo%Cano%' --test candidate
--and c."4 ref no numeric" = '2891'