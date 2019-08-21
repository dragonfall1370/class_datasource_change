
WITH temp as (
select cg.CLIENT_ID
--, case when cg.NAME in (select cg.NAME from PROP_CLIENT_GEN cg group by cg.name having count(*) > 1) then concat (cg.NAME,' - ',address.TOWN ) else cg.NAME end as NAME
, case when cg.NAME in (select cg.NAME from PROP_CLIENT_GEN cg group by cg.name having count(*) > 1) then concat (cg.NAME,' (-DUPLICATION-) ',address.STREET1 ) else cg.NAME end as NAME
, address.TOWN as 'company-locationCity'
, address.state as 'company-locationState'
, address.COUNTRY as '(company-locationCountry)'
--select *
from PROP_CLIENT_GEN cg
left join ENTITY_TABLE e ON e.ENTITY_ID = cg.REFERENCE
left join (select REFERENCE,CONFIG_NAME,STREET1,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address ON e.ENTITY_ID = address.REFERENCE
left join (select REFERENCE,CONFIG_NAME,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Fax') fax ON e.ENTITY_ID = fax.REFERENCE
--WHERE NAME like '%DUPPLICATION%'
--group by cg.name having count(*) > 1
--order by cg.name
)

select * from temp where NAME like '%DUPLICATION%'
ORDER BY NAME
------

--with 
--dup (NAME) as (select cg.NAME from PROP_CLIENT_GEN cg group by cg.name having count(*) > 1)

--select cg.CLIENT_ID, cg.NAME from PROP_CLIENT_GEN cg where cg.NAME in (select cg.NAME from PROP_CLIENT_GEN cg group by cg.name having count(*) > 1) 
--order by cg.NAME

