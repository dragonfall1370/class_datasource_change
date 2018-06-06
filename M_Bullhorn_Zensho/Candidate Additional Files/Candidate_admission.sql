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