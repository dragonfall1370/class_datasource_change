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

---Valid companies | Contact is Candidate
select f03.uniqueid
, f03."3 position alphanumeric"
, f03."1 job ref numeric"
, f03."2 company xref"
, f01."1 name alphanumeric" as contact_candidate_name
, f03."20 contact xref"
, f02."1 name alphanumeric" as company_name
from f03
left join f01 on f01.uniqueid = f03."20 contact xref"
left join f02 on f02.uniqueid = f03."2 company xref"
where "2 company xref" in (select uniqueid from f02)
and f01."101 candidate codegroup  23" = 'Y'


--Valid Companies | New contacts
select f03.uniqueid
, f03."3 position alphanumeric"
, f03."1 job ref numeric"
, f03."2 company xref"
, f01."1 name alphanumeric" as contact_candidate_name
, f03."20 contact xref"
, f02."1 name alphanumeric" as company_name
from f03
left join f01 on f01.uniqueid = f03."20 contact xref"
left join f02 on f02.uniqueid = f03."2 company xref"
where "2 company xref" in (select uniqueid from f02)
and f03."20 contact xref" is not NULL
and f01.uniqueid is NULL --aka new contacts

--Invalid companies | new contacts
select f03.uniqueid
, f03."3 position alphanumeric"
, f03."1 job ref numeric"
, f03."2 company xref"
, f01."1 name alphanumeric" as contact_candidate_name
, f03."20 contact xref"
, f02."1 name alphanumeric" as company_name
from f03
left join f01 on f01.uniqueid = f03."20 contact xref"
left join f02 on f02.uniqueid = f03."2 company xref"
where "2 company xref" not in (select uniqueid from f02)
and f03."20 contact xref" is not NULL --New contacts


--Invalid companies | valid contacts
select f03.uniqueid
, f03."3 position alphanumeric"
, f03."1 job ref numeric"
, f03."2 company xref"
, f01."1 name alphanumeric" as contact_candidate_name
, f03."20 contact xref"
from f03
left join f01 on f01.uniqueid = f03."20 contact xref"
where "2 company xref" not in (select uniqueid from f02)
and f01.uniqueid is not NULL --(images)


--Invalid companies | invalid contacts
select f03.uniqueid
, f03."3 position alphanumeric"
, f03."1 job ref numeric"
, f03."2 company xref"
, f03."20 contact xref"
from f03
where "2 company xref" not in (select uniqueid from f02)
and f03."20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y')