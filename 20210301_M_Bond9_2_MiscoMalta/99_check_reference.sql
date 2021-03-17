--Schema check
select table_name, column_name, ordinal_position
FROM information_schema.columns
where table_schema = 'public'
order by table_name, ordinal_position

--Check reference
select *
from f01

select *
from f01docs1

select *
from f02

select *
from f03

select *
from f04
where "3 job ref xref" is not NULL

select *
from f05

select *
from f06

select *
from f07

select *
from f08

select *
from f12

select *
from f13

select *
from f17

select *
from f22

select *
from f23

select *
from codes
where codegroup = '68'

select * from codes where Codegroup = '96'

select *
from f05
where uniqueid = '80810301DBB48080'


select f13."19 status codegroup  68"
, f13."15 last actio codegroup  94"
, t68.*
, t94.*
from f13
left join (select * from codes where codegroup = '68') t68 on t68.code = f13."19 status codegroup  68"
left join (select * from codes where codegroup = '94') t94 on t94.code = f13."15 last actio codegroup  94"


--CHECK SKILLS / INDUSTRY / CATEGORIES
select sub.uniqueid
, sub."1 attribute alphanumeric" as sub_type
, case sub."11 rootkey codegroup  58"
		when 'A' then 'Skills'
		when 'B' then 'Industry'
		when 'C' then 'Categories'
		end as code_type
, sub."2 parent xref"
, main."1 attribute alphanumeric" as main_type
from f12 sub
left join f12 main on main.uniqueid = sub."2 parent xref"
where 1=1
--and main."2 parent xref" is NULL --combination with codegroup
--and main."11 rootkey codegroup  58" = 'B' --Industry
--and main."11 rootkey codegroup  58" = 'A' --Skills
--and main."11 rootkey codegroup  58" = 'C' --Categories
--and main."11 rootkey codegroup  58" in ('A', 'B', 'C')
--and main."11 rootkey codegroup  58" not in ('A', 'B', 'C') --Other types
--and sub."2 parent xref" is not NULL --checking hierachy mapping
order by main."1 attribute alphanumeric", sub."1 attribute alphanumeric"


--DISTINCT ADDRESS TO GET COUNTRY IF MISSING
select distinct right(c."25 address alphanumeric", position('~' in reverse(c."25 address alphanumeric")) - 1) 
from f02 c

--CHECK DOCUMENTS
select uniqueid
, "relative document path"
, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) as document
from f02docs2


--Contacts
select *
from f01 c
where c."100 contact codegroup  23" = 'Y' --1589


--Candidates
select *
from f01 c
where c."101 candidate codegroup  23" = 'Y' --30467


--Contacts links Candidates | 303
select *
from f01 c
where c."101 candidate codegroup  23" = 'Y'
and c."100 contact codegroup  23" = 'Y'


--IMPORTANT NOTES
/* 
f04.csv is work history > check format before restore
*/