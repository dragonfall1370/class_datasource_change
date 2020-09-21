

with
  contact0 (CLIENT,name,CONTACT,rn) as (SELECT cc.CLIENT, cg.name, cc.CONTACT,ROW_NUMBER() OVER(PARTITION BY CONTACT ORDER BY cc.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cc left join PROP_CLIENT_GEN cg on cg.reference = cc.client )
, contact as (select CLIENT, name, CONTACT from contact0 where rn = 1)
--select count(*) from contact --15702
--select * from contact where CONTACT = 38077

select
	  ccc.CONTACT as 'contact-externalId', pg.person_id
	, et.createddate as "insert_timestamp"
	  
from contact ccc
left join PROP_PERSON_GEN pg on ccc.CONTACT = pg.REFERENCE 
left join (
       select et.ENTITY_ID, et.createddate, et.created_by, eg1.name as created_by_name, et.UPDATEDDATE, et.UPDATED_BY, eg2.name as UPDATED_BY_NAME 
       from ENTITY_TABLE et 
       left join PROP_EMPLOYEE_GEN eg1 on eg1.user_ref = et.created_by
       left join PROP_EMPLOYEE_GEN eg2 on eg2.user_ref = et.UPDATED_BY
       ) et on et.ENTITY_ID = ccc.CONTACT
       