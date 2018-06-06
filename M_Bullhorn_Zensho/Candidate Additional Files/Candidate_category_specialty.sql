
with tmp_1(userid, categoryid) as
(SELECT userid,
LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS categoryID
FROM
(
SELECT userid,CAST('<XMLRoot><RowData>' + REPLACE(cast(categoryIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x
FROM  bullhorn1.Candidate
)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
, t1(userId, Categoryname) as (
select tmp_1.userid, CL.occupation
from tmp_1 inner join 
bullhorn1.BH_CategoryList CL ON tmp_1.categoryid = CL.categoryID
)

, CName(Userid, Name) as (SELECT
     Userid,
     STUFF(
         (SELECT DISTINCT ',' + Categoryname
          from  t1
          WHERE Userid = a.Userid
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM t1 as a
GROUP BY a.Userid)

select CA.userID, CA.candidateID, CA.categoryIDList, CName.name from bullhorn1.Candidate CA
left outer join CName on CA.userID = CName.Userid
--where CA.UserID = 130

----
----
select * from t1


----
----
select * from bullhorn1.BH_CategoryList
where 1=1
and categoryID = 1181935
name like '10589%'

----
with tmp_2(userid, SpecialtyID) as
(SELECT userid,
LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS SpecialtyID
FROM
(
SELECT userid,CAST('<XMLRoot><RowData>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x
FROM  bullhorn1.Candidate
)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
, t2(userId, Specialtyname) as (
select tmp_2.userid, VS.name
from tmp_2 inner join 
bullhorn1.View_Specialty VS ON tmp_2.SpecialtyID = VS.specialtyID
)

, SpeName(Userid, Name) as (SELECT
     Userid,
     STUFF(
         (SELECT DISTINCT ',' + Specialtyname
          from  t2
          WHERE Userid = b.Userid
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM t2 as b
GROUP BY b.Userid)

select CA.userID, CA.candidateID, CA.specialtyIDList, SpeName.name from bullhorn1.Candidate CA
left outer join SpeName on CA.userID = SpeName.Userid
---where CA.userID = 130


-----
-----
-----
select CA.candidateID, UCOI.instanceID, COI.text1, COI.text2, concat(text1,' ',text2) as 'Text' from bullhorn1.Candidate CA
left outer join bullhorn1.BH_UserCustomObjectInstance UCOI on CA.userID = UCOI.userID
left outer join bullhorn1.BH_CustomObjectInstance COI on UCOI.instanceID = COI.instanceID

----

with t1(userId, CombinedText) as (
select UCOI.userID, concat(text1,' ',text2) as CombinedText
from bullhorn1.BH_UserCustomObjectInstance UCOI
inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID
)
, admission(Userid, Admission) as (SELECT
     Userid,
     STUFF(
         (SELECT DISTINCT '|' + CombinedText
          from  t1
          WHERE Userid = a.Userid
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM  t1 as a
GROUP BY a.Userid)

select CA.candidateID, Ad.Admission, CA.userID from bullhorn1.Candidate CA
left outer join admission Ad ON CA.userID = Ad.Userid
--where CA.userID = 14138




select * from bullhorn1.BH_UserCustomObjectInstance

select * from bullhorn1.BH_CustomObjectInstance

select * from bullhorn1.BH_JobResponse

select CA.userID, CA.candidateID, CA.customText2 as GeneralWorkFunction, CA.businessSectorIDList, BSL.name as Industry
, CA.categoryID, CA.categoryIDList, CL.name, CA.specialtyIDList, VS.name
from bullhorn1.Candidate CA
left outer join bullhorn1.BH_BusinessSectorList BSL 
on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
left outer join bullhorn1.BH_CategoryList CL on cast(CA.categoryIDList as varchar) = cast(CL.categoryID as varchar)
left outer join bullhorn1.View_Specialty VS on cast(CA.specialtyIDList as varchar) = cast(VS.specialtyID as varchar)

select * from bullhorn1.View_Specialty



SELECT userid,
LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS categoryID
FROM
(
SELECT userid,CAST('<XMLRoot><RowData>' + REPLACE(cast(categoryIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x
FROM  bullhorn1.Candidate
)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)