
select skillSet, skillIDList, * from bullhorn1.Candidate C where C.userid in (161358)

-- SFE v1
/*
with -- SkillName: split by separate rows by comma, then combine them into SkillName
  SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '')
--select count(*) from SkillName where SkillName <> '' and SkillName is not null --123434
--select top 300 * from SkillName
--select count(distinct ltrim(SkillName)) as Skill from SkillName --where SkillName
--select distinct ltrim(SkillName) as Skill from SkillName --where SkillName


select top 10 
       C.candidateid
       , 3812 as FE
       , sfe.SkillName
--select count(*) --131654
FROM  bullhorn1.Candidate C 
left join SkillName sfe on sfe.userid = C.userid
where isdeleted <> 1 and status <> 'Archive'
and sfe.SkillName <> '' and sfe.SkillName is not null
and C.userid in (161358)


select sfe.SkillName, count(*)
FROM  bullhorn1.Candidate C
left join SkillName sfe on sfe.userid = C.userid
where isdeleted <> 1 and status <> 'Archive'
and sfe.SkillName <> '' and sfe.SkillName is not null
group by sfe.Skillname
*/



-- SFE v2
/*
with t as (
       SELECT [BH_Candidate].[candidateID]
              ,[BH_Candidate].[userID]
              ,[BH_Candidate].[type]
              ,[BH_Candidate].[status]
              ,[BH_Candidate].[dateAdded]
              ,[BH_User].name as uname
              ,[BH_UserContact].name as usname
              ,[BH_UserSkill].skillID
              ,[BH_SkillList].name
       FROM [bullhorn1].[BH_Candidate]
       LEFT JOIN [bullhorn1].[BH_User] on [BH_User].userID = [BH_Candidate].userID
       LEFT JOIN [bullhorn1].BH_UserContact on BH_User.userID = BH_UserContact.userID
       LEFT JOIN [bullhorn1].BH_UserSkill on BH_Candidate.userID = BH_UserSkill.userID
       LEFT JOIN [bullhorn1].BH_SkillList on BH_UserSkill.skillID = BH_SkillList.skillID
)

, SkillName(candidateID, userId, SkillName) as (SELECT candidateID, userId, name from t WHERE t.name <> '')

select top 100 * from SkillName where userid in (161358,165199)
*/



-- SFE v3
with t as (
       SELECT [BH_Candidate].candidateID
              ,[BH_Candidate].userID
              ,[BH_UserSkill].skillID
              ,[BH_SkillList].name
       FROM [bullhorn1].[BH_Candidate]
       LEFT JOIN [bullhorn1].BH_User on [BH_User].userID = [BH_Candidate].userID
       LEFT JOIN [bullhorn1].BH_UserSkill on [BH_Candidate].userID = [BH_UserSkill].userID
       LEFT JOIN [bullhorn1].BH_SkillList on [BH_UserSkill].skillID = [BH_SkillList].skillID
       WHERE [BH_SkillList].name <> '' and [BH_Candidate].isdeleted <> 1 and [BH_Candidate].status <> 'Archive'
       --AND [BH_Candidate].[userID] in (165199)
)
--select top 1000 * from t where name <> ''
--select count(*) from t --69507
--select name, count(*) from t group by name
--select distinct(ltrim(rtrim(name))) from t where name not in ('.NET')

select top 100 * 
from t 
--where userid in (161358)
where userid in (165199)

