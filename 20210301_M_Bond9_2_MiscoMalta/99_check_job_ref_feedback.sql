select j.uniqueid
, j."3 position alphanumeric"
, j."2 company xref"
, f02."1 name alphanumeric"
, j."20 contact xref" 
, f01."1 name alphanumeric" contact_name
, coalesce(nullif("74 no reqd numeric", '')::int, 1) as "position-headcount"
, coalesce(to_date("21 order date date", 'DD/MM/YY'), to_date("23 start date", 'DD/MM/YY')) as "position-startDate"
--, to_date("28 created date", 'DD/MM/YY') as "position-startDate"
, to_date("24 end date", 'DD/MM/YY') as "position-endDate"
, j."1 job ref numeric" reference_number
, c5.description job_status
from f03 j
left join (select * from codes where codegroup = '6') c5 on c5.code = j."5 status codegroup   6" --status
left join (select * from codes where codegroup = '3') c3 on c3.code = j."4 type codegroup   3" --job type
left join (select * from codes where codegroup = '20') c20 on c20.code = j."40 reason vac codegroup  20" --job internal
left join (select * from codes where codegroup = '133') c133 on c133.code = j."167 jobemail codegroup 133" --job internal
left join (select * from codes where codegroup = '26') c26 on c26.code = j."83 locations codegroup  26" --job locations
left join (select uniqueid, "1 name alphanumeric", "100 contact codegroup  23", "101 candidate codegroup  23" from f01 where "100 contact codegroup  23" = 'Y') f01 on f01.uniqueid = j."20 contact xref" --contact reference
left join (select uniqueid, "1 name alphanumeric" from f02) f02 on f02.uniqueid = j."2 company xref"
where 1=1
--and "20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y') --1-Jobs with invalid contacts | 493
--and "20 contact xref" not in (select uniqueid from f01) --2-Jobs with empty contacts | 204
--and "2 company xref" in (select uniqueid from f02) ---3-Valid companies | Contact is Candidate
--and j."20 contact xref" in (select uniqueid from f01 where "101 candidate codegroup  23" = 'Y')
--and "2 company xref" in (select uniqueid from f02) --4-Valid Companies | New contacts | 491
--and j."20 contact xref" is not NULL
--and f01.uniqueid is NULL --aka new contacts
--and "2 company xref" not in (select uniqueid from f02) --5-Invalid companies | new contacts 
--and j."20 contact xref" in (select uniqueid from f01) --New contacts
--and "2 company xref" not in (select uniqueid from f02) --6-Invalid companies | invalid contacts
--and j."20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y')
--and "2 company xref" not in (select uniqueid from f02) --7-Invalid companies | invalid contacts
--and (j."20 contact xref" not in (select uniqueid from f01) or j."20 contact xref" is NULL)
--and "2 company xref" not in (select uniqueid from f02) --New companies from job
--and "2 company xref" in (select uniqueid from f02) ---Valid companies | Contact is Candidate
--and j."20 contact xref" not in (select uniqueid from f01 where "100 contact codegroup  23" = 'Y')