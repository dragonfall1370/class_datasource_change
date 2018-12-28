
-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
with
  BusinessSector0(userid, businessSectorID) as (SELECT userid, a.value as ind FROM  bullhorn1.Candidate m CROSS APPLY STRING_SPLIT( ltrim(rtrim( convert(varchar(100),m.businessSectorIDList) )), ',') AS a where isPrimaryOwner = 1 )
, BusinessSector(userId, BusinessSector) as (SELECT userId, ltrim(rtrim(BSL.name)) as BusinessSector from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' )
/*select 
       distinct BusinessSector
       , current_timestamp as insert_timestamp
from BusinessSector*/
  
 
SELECT
         CA.candidateID
       , BS.BusinessSector
from bullhorn1.Candidate CA
left join BusinessSector BS on CA.userID = BS.userId
where CA.isPrimaryOwner = 1



-- SOURCE
select distinct ltrim(rtrim(source)) as name
       , 1 as source_type
       , current_timestamp as insert_timestamp
       , 11 as payment_style 
from bullhorn1.Candidate C
where source <> '' and C.isPrimaryOwner = 1

select
	  C.candidateID as 'candidate-externalId'
--       , Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
--       , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
       , ltrim(rtrim(c.source)) as source
-- select count (*) -- select distinct ltrim(rtrim(source))
from bullhorn1.Candidate C
where source <> '' and C.isPrimaryOwner = 1
