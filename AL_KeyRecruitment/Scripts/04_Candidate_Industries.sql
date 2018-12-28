-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
drop table if exists #CanIndustriesTmp1
select x.UserId, trim(' ,' from isnull(cast(x.businessSectorIDList as varchar(max)), '')) as industryIds

into #CanIndustriesTmp1

from bullhorn1.Candidate x
where x.isPrimaryOwner = 1 and x.isDeleted = 0
--#debug
select * from #CanIndustriesTmp1

drop table if exists #CanIndustriesTmp2

select
userID,
iif(len(trim(isnull(value, ''))) = 0, 0, convert(int, trim(isnull(value, '')))) as industryId

into #CanIndustriesTmp2

from #CanIndustriesTmp1 x
    cross apply string_split(x.industryIds, ',');

drop table if exists #CanIndustriesTmp1

--#debug
--select * from #CanIndustriesTmp2

drop table if exists #CanIndustriesTmp3

select
x.userID
, trim(isnull(y.name, '')) as industryName

into #CanIndustriesTmp3

from #CanIndustriesTmp2 x
left join bullhorn1.BH_BusinessSectorList y ON x.industryId = y.businessSectorID

drop table if exists #CanIndustriesTmp2

--#debug
--select * from #CanIndustriesTmp3

drop table if exists VCCanIndustries

select
userID
, string_agg(industryName, ',') as Industries

into VCCanIndustries

from #CanIndustriesTmp3
group by userID

drop table if exists #CanIndustriesTmp3

select * from VCCanIndustries


--, BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
--, BusinessSector(userId, BusinessSector) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS URLList FROM BusinessSector0 as a where a.businessSectorID <> '' GROUP BY a.userId)