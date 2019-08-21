with tmp_1(userid, categoryid) as
(SELECT userid,
LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS categoryID
FROM
(
SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(categoryIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x
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
         (SELECT DISTINCT ', ' + Categoryname
          from  t1
          WHERE Userid = a.Userid
          FOR XML PATH (''))
          , 1, 2, '')  AS URLList
FROM t1 as a
GROUP BY a.Userid)

, tmp_2(userid, specialtyid) as
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
         (SELECT DISTINCT ', ' + Specialtyname
          from  t2
          WHERE Userid = b.Userid
          FOR XML PATH (''))
          , 1, 2 '')  AS URLList
FROM t2 as b
GROUP BY b.Userid)

, t3(userId, CombinedText) as (
select UCOI.userID, concat(text1,' ',text2) as CombinedText
from bullhorn1.BH_UserCustomObjectInstance UCOI
inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID
)
, admission(Userid, Admission) as (SELECT
     Userid,
     STUFF(
         (SELECT DISTINCT ' | ' + CombinedText
          from  t3
          WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> ''
          FOR XML PATH (''))
          , 1, 3, '')  AS URLList
FROM  t3 as c
GROUP BY c.Userid)


select CA.userID, CA.candidateID, AD.Admission, CA.customText2 as GeneralWorkFunction, CA.businessSectorIDList, BSL.name as Industry, 
CA.categoryIDList, replace(CName.name,'&amp;','&') as Practice_Area, CA.specialtyIDList, replace(SpeName.name,'&amp;','&') as Specialty
from bullhorn1.Candidate CA
left outer join admission AD on CA.userID = AD.Userid
left outer join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
left outer join CName on CA.userID = CName.Userid
left outer join SpeName on CA.userID = SpeName.Userid
---where CA.userID = 130
--where admission like '%|  |'