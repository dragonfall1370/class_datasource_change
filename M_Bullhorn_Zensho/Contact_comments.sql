with 
 t4(Userid, Notes) as (SELECT
     Userid,
     STUFF(
         (SELECT DISTINCT char(10) + convert(varchar(10), dateAdded, 120) + ' ' + cast(comments as varchar(max))
          from  [bullhorn1].[BH_UserComment]
          WHERE Userid = a.Userid
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM  [bullhorn1].[BH_UserComment] AS a
GROUP BY a.Userid)

select Cl.clientID, t4.Userid, left(t4.Notes, 30000)
from bullhorn1.BH_Client Cl 
left join t4 on Cl.userID = t4.Userid