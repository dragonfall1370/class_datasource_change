--select distinct  from Attributes



--select distinct CodeValue from CandAttributes

--select * from Candidates

select distinct Key_Word_1, a.Description
from Candidates c left join Attributes a on c.Key_Word_1 = a.Code
where Key_Word_1 <> ''


--select
--from CandAttributes left join 

with temp as (select LinktoUniqueID, a.Description
from CandAttributes ca left join Attributes a on ca.CodeValue = a.Code)

, fe as (SELECT LinktoUniqueID,
     STUFF(
         (SELECT ', ' + description
          from  temp
          WHERE LinktoUniqueID = t.LinktoUniqueID
    order by LinktoUniqueID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS fe
FROM temp as t
GROUP BY t.LinktoUniqueID)

select * from fe
