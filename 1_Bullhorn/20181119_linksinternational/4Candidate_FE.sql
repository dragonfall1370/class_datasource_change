
-- CATEGORY
with
  CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(concat(categoryID,',',categoryIDList) as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, ltrim(rtrim(CL.occupation)) as occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID where CateSplit.categoryid <> '')
--select distinct Name from CName;
--select concat('insert into functional_expertise(id,name) values (nextval(''functional_experties_id_seq''),''',a.name,''');') from (select distinct Name from CName) a;
--select distinct(Name), count(*) from CName group by Name;
--select count(*) from CName --123926
--select userid, categoryID,categoryIDList FROM  bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive' and userid in (165180,165184 ,165199, 161585)
select C.candidateID, C.userid, C.categoryID, C.categoryIDList, UC.name
FROM bullhorn1.Candidate C
left join ( select UC.userID, UC.categoryID, replace(CL.name,'850:','') as name from BULLHORN1.View_UserCategory UC left join bullhorn1.BH_CategoryList CL ON UC.categoryid = CL.categoryID ) UC on UC.userid = C.userID
where C.isdeleted <> 1 and C.status <> 'Archive' and C.userid in (165180,165184 ,165199, 161585)


-- SPECIALTY
, SpecSplit(userid, specialtyid) as (SELECT userid,Split.a.value('.','varchar(2000)') AS SpecialtyID FROM (SELECT userid,CAST('<M>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') t CROSS APPLY x.nodes('/M') as Split(a) )
, SpeName(Userid, Name) as (SELECT Userid, ltrim(rtrim(VS.name)) as name from SpecSplit left join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID WHERE SpecSplit.specialtyid <> '')
, SpeName0 as (select distinct Name from SpeName)
--select count(distinct Name) from SpeName
--select count(*) from SpeName --167879
SELECT C.userid, C.specialtyIDList FROM  bullhorn1.Candidate C left join bullhorn1.BH_UserContact UC on C.userID = UC.UserID where C.isdeleted <> 1 and C.status <> 'Archive' and C.userid in (165180,165184 ,165199, 161585)
select C.candidateID, C.userid, C.specialtyIDList, UC.SpecialtyID, UC.name
FROM bullhorn1.Candidate C
left join ( select US.userID, US.SpecialtyID, VS.name as name from BULLHORN1.View_UserSpecialty US left join bullhorn1.View_Specialty VS ON US.SpecialtyID = VS.specialtyID ) UC on UC.userid = C.userID
where C.isdeleted <> 1 and C.status <> 'Archive' and C.userid in (165180,165184 ,165199, 161585)


, fe0 as (select distinct Name from CName UNION select distinct Name from SpeName)
--select * from fe0
--select count(*) from fe0

--select concat('insert into functional_expertise(id,name) values (nextval(''functional_experties_id_seq''),''',name,''');') from SpeName0 where Name not in (select distinct Name from CName) 
--select concat('insert into functional_expertise(id,name) values (nextval(''functional_experties_id_seq''),''',name,''');') from fe0 where name is not null and name <> '' order by Name asc

, fe as (select * from CName UNION select * from SpeName)

select top 10 
       C.candidateid, fe.name
--select count(*) 
FROM  bullhorn1.Candidate C
left join fe on fe.userid = C.userid
where isdeleted <> 1 and status <> 'Archive'
and C.userid in (165180,165184 ,165199, 161585)

--select fe.name, count(*) FROM  bullhorn1.Candidate C left join fe on fe.userid = C.userid where isdeleted <> 1 and status <> 'Archive' group by fe.name








-- VERSION 2
-- CATEGORY
with
  CATEGORY(candidateID, userid, categoryIDList, categoryID, name) as (
       select C.candidateID, C.userid, C.categoryIDList, C.categoryID, UC.name
       FROM bullhorn1.Candidate C
       left join ( select UC.userID, UC.categoryID, replace(replace(replace(replace(replace(name,'4294:',''),'4295:',''),'4296:',''),'4297:',''),'850:','') as name from BULLHORN1.View_UserCategory UC left join bullhorn1.BH_CategoryList CL ON UC.categoryid = CL.categoryID ) UC on UC.userid = C.userID
       where C.isdeleted <> 1 and C.status <> 'Archive' and C.userid in (155843) --and C.userid in (165180,165184 ,165199, 161585)
       )

-- SPECIALTY
, SPECIALTY(candidateID, userid, specialtyIDList, SpecialtyID, name) as (
       select C.candidateID, C.userid, C.specialtyIDList, UC.SpecialtyID, UC.name
       FROM bullhorn1.Candidate C
       left join ( select US.userID, US.SpecialtyID, VS.name as name from BULLHORN1.View_UserSpecialty US left join bullhorn1.View_Specialty VS ON US.SpecialtyID = VS.specialtyID ) UC on UC.userid = C.userID
       where C.isdeleted <> 1 and C.status <> 'Archive' --and C.userid in (155843,165180,165184 ,165199, 161585)
       )

, fe0 as (select distinct replace(replace(replace(replace(name,'4294:',''),'4295:',''),'4296:',''),'4297:','') as name from CATEGORY UNION select distinct replace(replace(replace(replace(name,'4294:',''),'4295:',''),'4296:',''),'4297:','') from SPECIALTY)
--select * from fe0 where name not in ('.Net')
--select count(*) from fe0
--select concat('insert into functional_expertise(id,name) values (nextval(''functional_experties_id_seq''),''',name,''');') from SpeName0 where Name not in (select distinct Name from CName) 
--select concat('insert into functional_expertise(id,name) values (nextval(''functional_experties_id_seq''),''',name,''');') from fe0 where name is not null and name <> '' order by Name asc

, fe as (select * from CATEGORY UNION select * from SPECIALTY)

select --top 10 
       C.candidateid, fe.userid, fe.categoryIDList, fe.categoryID, fe.name
--select count(*) 
FROM  bullhorn1.Candidate C
left join fe on fe.userid = C.userid
where isdeleted <> 1 and status <> 'Archive' and fe.name is not null
and C.userid in (155738) --(165180,165184 ,165199, 161585)
--select fe.name, count(*) FROM  bullhorn1.Candidate C left join fe on fe.userid = C.userid where isdeleted <> 1 and status <> 'Archive' group by fe.name
