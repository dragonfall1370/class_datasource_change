/* List of Level 1 Attributes
Agency Sector (12 SFE)
Analytic Software (25 SFE)
Analytics (42 SFE)
B2b (8 SFE)
Business Services (2 SFE) 
Consumer Durables (7)
Creative (16)
Creative Software (44)
Database Programming Tools (12)
Databases (9)
Dev Ops (11 - level 2-3) --ID: 687
Digital (30)
Financial Services Sector (7)
Fmcg Sector (4)
Fr_tags (Marketing) (15)
International (6)
IT/TELCO Sector (4)
Languages (17)
Leisure / Travel Sector (8)
Management (8)
Marketing (41)
Professional Services Sector (6)
Programming & IT (51 + 6) --ID: 84
Public Services Sector (7)
Qualifications (2 + 10) --ID: 111
Retail Sector (10)
Sales (5)
Social Media (6)
Web Analytics (9)

--NOT INCLUDED FROM DATABASE
Database Parser Links - ID: 7
Experience - ID: 37
Project Management - ID: 70
Candidate Tier - ID: 542 
*/

/* COUNT REFERENCES */

with ABC as (select b.AttributeUniq, count(*) as COUNTSFE
--b.AttributeUniq as FEID
--, b.DisplayCode as FECode
--, b.LongDescription as FEValue
--, a.AttributeUniq as SFEID
--, a.DisplayCode as SFECOde
--, a.LongDescription as SFEValue
from Attributes a
left join Attributes b on a.ParentAttUniq = b.AttributeUniq
where a.Level = 2
and a.ParentAttUniq not in (7, 37, 70, 542)
group by b.AttributeUniq)

select ABC.COUNTSFE, a.LongDescription, A.AttributeUniq 
from Attributes a
left join ABC on ABC.AttributeUniq = a.AttributeUniq
where ABC.COUNTSFE is not NULL
order by a.LongDescription

/* REFERENCE CHECK */

select * from Attributes
where ParentAttUniq = 687
order by LongDescription --Dev Ops remove this value

select * from Attributes
where ParentAttUniq = 688
order by LongDescription --Dev Ops | 12 values -> not taking ID 689

select * from Attributes
where ParentAttUniq = 84
order by LongDescription --Programming & IT | 51 values

select * from Attributes
where ParentAttUniq = 84
order by LongDescription --Programming & IT | 51 values

select * from Attributes
where LongDescription like '%Extreme%'

select * from Attributes
where ParentAttUniq = 70
order by LongDescription --Project Management > 6 values

select * from Attributes
where AttributeUniq = 70 --Project Management

select * from Attributes
where ParentAttUniq = 177
order by LongDescription --Professional Services Sector

select * from Attributes
where ParentAttUniq = 111
order by LongDescription --Qualifications | 2 rows

select * from Attributes
where ParentAttUniq in (112, 116)
order by LongDescription --From Qualifications | 10 rows

---ATTRIBUTES LEVEL 1 (FE)
select * from Attributes
where Level = 1 -- 33 values | 4 excluded values

---ATTRIBUTES LEVEL 2 (SFE)
select * from Attributes
where Level = 2
and ParentAttUniq not in (7, 37, 70, 542)
order by ParentAttUniq, LongDescription -- 437 values

---FE TO INJECT
select AttributeUniq
, DisplayCode
, LongDescription as ForwardFE
, getdate() as Forward_insert_timestamp
from Attributes
where Level = 1
and AttributeUniq not in (7, 37, 70, 542) -- 29 values

---SFE TO INJECT
---Dev Ops
with DevOps as (select 688 as FEID
	, 'FRGRPS' as FECode
	, 'Dev Ops' as FEValue
	, AttributeUniq as SFEID
	, DisplayCode as SFECode
	, LongDescription as SFEValue
	from Attributes
	where ParentAttUniq = 688 --Dev Ops
	and AttributeUniq <> 689)

--Programming & IT
, ProjectMgmt as (select 84 as FEID
	, 'PRGRMNGT' as FECode
	, 'Programming & IT' as FEValue
	, AttributeUniq as SFEID
	, DisplayCode as SFECode
	, LongDescription as SFEValue
	from Attributes
	where ParentAttUniq = 70) --Project Management

, Programing as (select b.AttributeUniq as FEID
	, b.DisplayCode as FECode
	, b.LongDescription as FEValue
	, a.AttributeUniq as SFEID
	, a.DisplayCode as SFECode
	, a.LongDescription as SFEValue
	from Attributes a
	left join Attributes b on a.ParentAttUniq = b.AttributeUniq
	where a.Level = 2
	and a.ParentAttUniq = 84
	UNION ALL
	select * from ProjectMgmt)

--Qualifications
, CertGraduate as (select 111 as FEID
	, 'QLFCTNS' as FECode
	, 'Qualifications' as FEValue
	, AttributeUniq as SFEID
	, DisplayCode as SFECode
	, LongDescription as SFEValue
	from Attributes
	where ParentAttUniq in (112, 116)) --Cert Engineering | Graduate

, Qualifications as (select b.AttributeUniq as FEID
	, b.DisplayCode as FECode
	, b.LongDescription as FEValue
	, a.AttributeUniq as SFEID
	, a.DisplayCode as SFECode
	, a.LongDescription as SFEValue
	from Attributes a
	left join Attributes b on a.ParentAttUniq = b.AttributeUniq
	where a.Level = 2
	and a.ParentAttUniq = 111
	UNION ALL
	select * from CertGraduate)

, AFinalAttributes as (select b.AttributeUniq as FEID
	, b.DisplayCode as FECode
	, b.LongDescription as FEValue
	, a.AttributeUniq as SFEID
	, a.DisplayCode as SFECode
	, a.LongDescription as SFEValue
	from Attributes a
	left join Attributes b on a.ParentAttUniq = b.AttributeUniq
	where a.Level = 2 and b.Level = 1
	and a.ParentAttUniq not in (7, 37, 70, 542, 687, 84, 111)
	and a.AttributeUniq not in (7, 37, 70, 542, 687, 84, 111, 689, 112, 116)

UNION ALL
select * from DevOps

UNION ALL
select * from Programing

UNION ALL
select * from Qualifications)


---CREATE TEMP FINAL ATTRIBUTES
create table FinalAttributes (
	FEID int,
	FECode varchar(10),
	FEValue varchar(200),
	SFEID int,
	SFECode varchar(10),
	SFEValue varchar(200)
)

---INSERT VALUES INTO FINAL ATTRIBUTES
INSERT INTO FinalAttributes
select * from AFinalAttributes