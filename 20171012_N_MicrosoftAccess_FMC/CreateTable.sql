 create table Industry
(ApplicantId int PRIMARY KEY,
CanIndustry nvarchar(255),
)
go
select * from Industry

------
SELECT concat('FMC',ApplicantId) as CanExternalId,
 convert(int,Split.a.value('.', 'VARCHAR(100)')) AS IndustryID 
 FROM (SELECT ApplicantId, CAST ('<M>' + REPLACE(CanIndustry,',','</M><M>') + '</M>' AS XML) AS Data FROM Industry) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
 --where a.ApplicantId = 176
order by ApplicantId
