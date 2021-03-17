with 
--DEFAULT CONTACT IF NON-EXISTING / INVALID CONTACTS IN JOBS
default_contact as (select distinct f03.uniqueid
	, f03."2 company xref" as company_id
	, "20 contact xref"
	, 'DEF' || f03."2 company xref" as contact_id
	, 'Default contact [Company Ref ' || f02."6 ref no numeric" || ']' as contact_lastname
	from f03
	left join f02 on f02.uniqueid = f03."2 company xref"
	where 1=1
	and (nullif("20 contact xref", '') is NULL OR "20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y'))
	and "2 company xref" in (select uniqueid from f02) --244
	)

/* AUDIT CHECK CONTACTS JOBS
--Candidates as job contacts
	select f03.uniqueid
	, "2 company xref"
	, "20 contact xref"
	, f01."100 contact codegroup  23"
	, f01."101 candidate codegroup  23"
	from f03
	left join f01 on f01.uniqueid = f03."20 contact xref"
	where 1=1
	and nullif("20 contact xref", '') is not NULL
	and "101 candidate codegroup  23" = 'Y' and "100 contact codegroup  23" = 'N' --candidates only
	--and "20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y') --check if contact exists as contact
	
--Jobs with invalid contacts | 493
select *
from f03
where "20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y')

--Jobs with empty contacts | 204
select *
from f03
where "20 contact xref" not in (select uniqueid from f01)
*/

--NEW COMPANIES FROM JOBS
, new_company as (select distinct f03.uniqueid
	, 'NEW_' || rank() over(order by "2 company xref") as com_ext_id
	, 'Company: ' || "2 company xref" as company_name
	, "2 company xref"
	, 'DEF_' || 'NEW_' || rank() over(order by "2 company xref") as con_ext_id
	, 'Default contact - ' || "2 company xref" as contact_last_name
	from f03
	where 1=1
	--and nullif("20 contact xref", '') is NULL
	and "2 company xref" not in (select uniqueid from f02) --7
	order by "2 company xref"
	)
	
--NEW CONTACTS FROM JOBS (valid contact name)
, new_contact as (select f03.uniqueid
	, f03."3 position alphanumeric"
	, f03."1 job ref numeric"
	, f03."2 company xref"
	, f02."1 name alphanumeric" as company_name
	, 'DEF_' || 'NEW_' || f03."2 company xref" || '_' || rank() over(partition by f03."2 company xref" order by "20 contact xref") as con_ext_id
	, f03."20 contact xref"
	, f01."1 name alphanumeric" as contact_candidate_name
	from f03
	left join f01 on f01.uniqueid = f03."20 contact xref"
	left join f02 on f02.uniqueid = f03."2 company xref"
	where "2 company xref" in (select uniqueid from f02)
	and f03."20 contact xref" is not NULL
	and f01.uniqueid is NULL --aka new contacts
	and position(' ' in "20 contact xref") > 1
	) --select * from new_contact where uniqueid = '80810301EB988080'

--DOCUMENTS | no document | file > f03docs3.csv | CHANGED TO NEW APPROACH

--MAIN SCRIPT
select j.uniqueid as "position-externalId"
, j."2 company xref"
, j."20 contact xref"
, case 
		when j.uniqueid in (select uniqueid from new_company) then nc.con_ext_id
		when j.uniqueid in (select uniqueid from new_contact) then ncon.con_ext_id
		when j.uniqueid in (select uniqueid from default_contact) then dc.contact_id
		when j."20 contact xref" is NULL or j."20 contact xref" not in (select uniqueid from f01) or j."2 company xref" not in (select uniqueid from f02) then 'MM999999999' --default contact | default company
		else j."20 contact xref" end as "position-contactId"
, coalesce(j."3 position alphanumeric", 'No job title') || ' [' || j."1 job ref numeric" || ']' as "position-title"
, f17."72 email add alphanumeric" as "position-owners"
, case
		when c3.code = '1' then 'CONTRACT'
		when c3.code = '2' then 'PERMANENT'
		when c3.code = '3' then 'CONTRACT'
		when c3.code = '4' then 'CONTRACT'
		else 'PERMANENT' end as "position-type"
, case 
		when "70 perm codegroup  33" = 'Y' then 'FULL_TIME'
		when "71 temp codegroup  34" = 'Y' then 'PART_TIME'
		when "15 temp-perm codegroup  23" = 'Y' then 'PART_TIME'
		else 'FULL_TIME' end as "position-employmentType"
, coalesce(nullif("74 no reqd numeric", '')::int, 1) as "position-headcount"
, coalesce(to_date("21 order date date", 'DD/MM/YY'), to_date("23 start date", 'DD/MM/YY')) as "position-startDate"
--, to_date("24 end date", 'DD/MM/YY') as "position-endDate"
--BUSINESS RULE ON ENDDATE
, case when c5.description = 'Closed' then coalesce(to_date("24 end date", 'DD/MM/YY'), now() - interval '1 day')::date
	else coalesce(to_date("24 end date", 'DD/MM/YY'), to_date("21 order date date", 'DD/MM/YY') + interval '1 year')::date end as "position-endDate"
--DESCRIPTION
, "40 reason vac codegroup  20"
, concat_ws(chr(10)
	, coalesce('Vacancy reason: ' || c20.description, NULL)
	, coalesce('Email template: ' || c133.description, NULL)
	) as "position-internalDescription"
, coalesce('Vacancy reason: ' || c20.description, NULL) as "position-publicDescription"
--COMPENSATION
, coalesce(nullif(split_part(j."32 sal from numeric", '~', 1), '')::float, 0) as "position-salaryFrom"
, coalesce(nullif(split_part(j."32 sal from numeric", '~', 2), '')::float, 0) as "position-salaryTo"
--, "155 pay rate numeric" as "position-payRate" --invalid format
--NOTE
, concat_ws(chr(10)
	, coalesce('Reference Number: ' || j."1 job ref numeric", NULL)
	, coalesce('Status: ' || c5.description, NULL)
	, coalesce('Locations: ' || c26.description, NULL)
	, coalesce('Salary: ' || "53 agreed sal numeric", NULL)
	, coalesce('Fee: ' || "16 agreed fee numeric", NULL)
	, coalesce('Close date: ' || to_date("165 closedate date", 'DD/MM/YY'), NULL)
	) as "position-note"
from f03 j
left join (select * from codes where codegroup = '6') c5 on c5.code = j."5 status codegroup   6" --status
left join (select * from codes where codegroup = '3') c3 on c3.code = j."4 type codegroup   3" --job type
left join (select * from codes where codegroup = '20') c20 on c20.code = j."40 reason vac codegroup  20" --job internal
left join (select * from codes where codegroup = '133') c133 on c133.code = j."167 jobemail codegroup 133" --job internal
left join (select * from codes where codegroup = '26') c26 on c26.code = j."83 locations codegroup  26" --job locations
left join f17 on f17.uniqueid = j."7 consultant xref" --job owner
left join new_company nc on nc.uniqueid = j.uniqueid
left join default_contact dc on dc.uniqueid = j.uniqueid
left join new_contact ncon on ncon.uniqueid = j.uniqueid
--where j.uniqueid = '80810301D8A28080'