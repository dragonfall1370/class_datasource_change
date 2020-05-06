select *
from CommunicationTypes

select *
from CustomDefinedColumns
where TableName = 'CLIENTSECTORDEFINEDCOLUMNS'

select *
from CustomColumnGroups

select *
from ClientSectorDefinedColumns

--CHECK DELETED RECORDS
select *
from SectorObjects

select *
from Sectors
/*
47 Main Sector
48 Deleted Candidates
49 Deleted Clients
50 Old Jobs */

select SectorId, count(SectorId)
from SectorObjects
group by SectorId
--47	63575
--48	18638
--49	402
--50	254

--CHECK OBJECTS COUNT

select ObjectID, count(ObjectTypeId)
from Objects
group by ObjectID
having count(ObjectTypeId) > 1
--1	APP Applicant
--2	CLNT Client
--3	CCT Contact
--4	PER Person

select ObjectTypeId, count(ObjectID)
from Objects
group by ObjectTypeId
--1	55183 Applicant
--2	15993 Client
--3	35647 Contact
--4	52 Person

select *
from Clients --15993

select count(*)
from Applicants --55205

select *
from clientcontacts --35812