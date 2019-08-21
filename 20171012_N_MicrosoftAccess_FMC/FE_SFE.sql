-- create table FunctionExpertise
--(CandidateId int PRIMARY KEY,
--FE nvarchar(255),
--SubFE nvarchar(255),
--)
--go
--select distinct(functionalExpertise) from FunctionExpertiseTypes
--with tempFEtypes as (SELECT concat('FMC',CandidateId) as CanExternalId, Split.a.value('.', 'VARCHAR(100)') AS FE 
-- FROM (SELECT CandidateId, CAST ('<M>' + REPLACE(FunctionalExpertise,',','</M><M>') + '</M>' AS XML) AS Data FROM FunctionExpertiseTypes) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
-- --where a.ApplicantId = 176
--)
--select distinct(FE) from tempFEtypes 
--ALTER TABLE FunctionExpertiseTypes
--ADD SubFE nvarchar(255);
--select Candidate from FunctionExpertise group by FE

--SELECT CandidateId, FE, SubFE, ROW_NUMBER() OVER(PARTITION BY FE ORDER BY SubFE ASC) AS rn 
--FROM FunctionExpertise

--select distinct(FE) from FunctionExpertise

with Temp_FETypes as (
select CandidateId, SubFE,
case when FE is null then 'Others'
else FE end as FE
from FunctionExpertise)

select CandidateId, tfe.FE, tfe.SubFE, fet.FEId
from Temp_FETypes tfe left join FETypes fet on tfe.FE = fet.FEName where tfe.SubFE is not null-- where tfe.FE = 'Others'
order by fet.FEId

-- select distinct(FE) from Temp_FETypes where FE = ''
-- -- where FE is not null and SubFE is not null

-- select * from FunctionExpertise order by FE

--create table FETypes
--(FEId int PRIMARY KEY,
--FEName nvarchar(255),
--)
--go

--with Temp_FETypes as (
--select SubFE,
--case when FE is null then 'Others'
--else FE end as FE
--from FunctionExpertise)

--, uniqueFE as (select fet.FEId, tfe.SubFE, ROW_NUMBER() OVER(PARTITION BY tfe.FE, tfe.SubFE ORDER BY tfe.FE ASC) AS rn
--from Temp_FETypes tfe left join FETypes fet on tfe.FE = fet.FEName where tfe.SubFE is not null)

--select * from uniqueFE where rn = 1


--, pair as (select tfe.SubFE, fet.FEId, ROW_NUMBER() OVER(PARTITION BY tfe.SubFE ORDER BY tfe.FE ASC) AS rn
--from Temp_FETypes tfe left join FETypes fet on tfe.FE = fet.FEName where tfe.SubFE is not null)
--select * from pair where rn = 1 and SubFE = 'motor'
--order by SubFE


--select * from Temp_FETypes

--create table SubFETypes
--(SubFEId int PRIMARY KEY,
--SubFEName nvarchar(255),
--)
--go

--select * from SubFETypes

with Temp_FETypes as (
select CandidateId, SubFE,
case when FE is null then 'Others'
else FE end as FE
from FunctionExpertise)

, temp_SubFE1 as(select CandidateId, tfe.FE, tfe.SubFE, fet.FEId
from Temp_FETypes tfe left join FETypes fet on tfe.FE = fet.FEName)

, temp_SubFE as(select CandidateId, tsfe.FE, tsfe.SubFE, tsfe.FEId, sfe.SubFEId, sfe.SubFEName
from temp_SubFE1 tsfe left join SubFETypes sfe on tsfe.SubFE = sfe.SubFEName and tsfe.FEId = sfe.FEId)
--where fet.FEId is not null-- where tfe.SubFE is not null-- where tfe.FE = 'Others'
--order by --tfe.CandidateId
select * from Temp_SubFE
---------FE and SubFE
with Temp_FETypes as (
select CandidateId, SubFE,
case when FE is null then 'Others'
else FE end as FE
from FunctionExpertise)

, temp_SubFE1 as(select CandidateId, tfe.FE, tfe.SubFE, fet.FEId
from Temp_FETypes tfe left join FETypes fet on tfe.FE = fet.FEName)

, temp_SubFE as(select CandidateId, tsfe.FEId, sfe.SubFEId
from temp_SubFE1 tsfe left join SubFETypes sfe on tsfe.SubFE = sfe.SubFEName and tsfe.FEId = sfe.FEId)
--where fet.FEId is not null-- where tfe.SubFE is not null-- where tfe.FE = 'Others'
--order by --tfe.CandidateId
select concat('FMC',CandidateId) as ExternalId, FEId,SubFEId from Temp_SubFE