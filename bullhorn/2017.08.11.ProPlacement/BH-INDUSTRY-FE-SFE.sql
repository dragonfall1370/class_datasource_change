
-- Business Sector(Industry)
with BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' )
select distinct cast(businessSectorIDList as varchar(max)) from bullhorn1.Candidate
select * from BusinessSector  where userid = '128424'
select distinct businessSectorID from BusinessSector0
select * from bullhorn1.BH_BusinessSectorList >>> VC vertical.name



with
-- CATEGORY - VC FE info
SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '' )
--select * from bullhorn1.BH_SkillList (156) >>> VC functional_expertise.name

-- SPECIALTY - VC SFE info
, CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID WHERE CateSplit.categoryid <> '' )
--select * from  CateSplit
select * from bullhorn1.BH_CategoryList (433) >>> VC sub_functional_expertise.name

select distinct fe.SkillName as fe, sfe.name as sfe, ca.userid, ca.candidateID as external_id, ca.firstname, ca.lastname
--select top 10 * 
from bullhorn1.Candidate ca
left join SkillName fe on fe.userid = ca.userID
left join CName sfe on sfe.userid = ca.userID
where ca.candidateID = 104
--9096

