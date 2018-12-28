
with
ind (clientCorporationID,ind) as (SELECT clientCorporationID, ind.value as ind FROM bullhorn1.BH_ClientCorporation m CROSS APPLY STRING_SPLIT( ltrim(rtrim( convert(varchar(100),m.businessSectorList) )), ';') AS ind)

/*select
       distinct ind as 'company-industry'
       , current_timestamp as insert_timestamp
from ind
where ind <> ''
*/

select 
        clientCorporationID
       , ind as 'company-industry' 
from ind
where ind <> ''




-- TV Business Area
with
t (clientCorporationID,customText1) as (SELECT clientCorporationID, customText1.value as customText1 FROM bullhorn1.BH_ClientCorporation m CROSS APPLY STRING_SPLIT( ltrim(rtrim( convert(varchar(100),m.customText1) )), ',') AS customText1 where customText1 <> '' )

--select
--        clientCorporationID as 'externalId'
--        , customText1  as 'TV Business Area'
select distinct customText1
from t


SELECT
         clientCorporationID as 'external_additional_id'
        --, name 
        , 'add_com_info' as additional_type
        , 1006 as form_id
        , 1018 as field_id
        , replace(replace(replace(replace( ltrim(rtrim(customText1)),'Advisory',1),'General',2),'Investment',3),'TT Client',4) as field_value
from bullhorn1.BH_ClientCorporation
where customText1 <> ''
