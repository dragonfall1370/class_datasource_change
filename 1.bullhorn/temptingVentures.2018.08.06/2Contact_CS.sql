
-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
with
  BusinessSector0(userid, businessSectorID) as (SELECT userid, a.value as ind FROM  ( select Cl.userID, UC.businessSectorIDList from bullhorn1.BH_Client Cl left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID where Cl.isPrimaryOwner = 1 and convert(varchar(100),UC.businessSectorIDList) <> '' and UC.businessSectorIDList is not null ) m CROSS APPLY STRING_SPLIT( ltrim(rtrim( convert(varchar(100),m.businessSectorIDList) )), ',') AS a )
, BusinessSector(userId, BusinessSector) as (SELECT userId, ltrim(rtrim(BSL.name)) as BusinessSector from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' )
-- select * from BusinessSector
/*select
       distinct BusinessSector
       , current_timestamp as insert_timestamp
from BusinessSector*/

SELECT
         Cl.clientID as 'contact-externalId'
--     , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
--	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , BS.BusinessSector
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join BusinessSector BS on Cl.userID = BS.userId
where cl.isPrimaryOwner = 1 and BS.BusinessSector is not null



-- TV Business Area
select
        Cl.clientID as 'external_additional_id'
        , 'add_con_info' as additional_type
        , 1007 as form_id
        , 1019 as field_id
        , replace(replace(replace(replace( ltrim(rtrim(UC.customText1)),'Advisory',1),'General',2),'Investment',3),'TT Client',4) as field_value
-- select distinct ltrim(rtrim(UC.customText1))
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where Cl.isPrimaryOwner = 1 and UC.customText1 <> ''


-- FE
select 
       Cl.clientid
       --, UC.categoryID
       , cat.occupation
from bullhorn1.BH_Client Cl 
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID 
left join bullhorn1.BH_CategoryList cat ON cat.categoryID = UC.categoryID
where Cl.isPrimaryOwner = 1
