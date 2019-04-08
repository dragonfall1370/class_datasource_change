

-- SKILL
select top 100
       can.contactid as 'candidate-externalId'
       , s.skill
--select count(*) --164.695
from dbo.candidates can
left join ( select si.objectid, s.skill from skillinstances si left join Skills s on s.skillid = si.skillid ) s on s.objectid = can.contactid
where s.objectid  is not null

select top 100 * from Skills
select top 100 * from skillinstances 
select s.skill, count(*) from skillinstances si left join Skills s on s.skillid = si.skillid group by s.skill
select * from Skills where skill in ('CCP IA','.Net Core','Advertising')


--SECTOR
select top 100 * from dbo.sectors
select distinct sector from dbo.sectors
select top 100 * from dbo.sectorinstances
select se.sector, count(*) from sectors se left join sectorinstances s on s.sectorid = se.sectorid group by se.sector


select se.sector, sk.skill
from sectors se
left join skills sk on sk.sectorid = se.sectorid
where sk.SectorId <> '' and sk.SectorId is not null;


-- Segments
select top 100 
       *
from dbo.sectors se
left join dbo.segments seg on seg.sectorID = se.sectorid






SELECT  Distinct      Contacts.DisplayName, Sectors.Sector, Segments.Segment, Contacts.ContactId
FROM            Segments INNER JOIN
                         SgmtInstances ON Segments.SegmentId = SgmtInstances.SegmentId INNER JOIN
                         Sectors ON Segments.SectorId = Sectors.SectorId INNER JOIN
                         Contacts ON SgmtInstances.ObjectId = Contacts.ContactId
Union
Select Distinct DisplayName, Sector, '', ContactId from Contacts Where Sector <> ''
Order By Sector