with
  contact0 (CLIENT,CONTACT,rn) as (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg)

, contact as (select CLIENT,CONTACT from contact0 where rn = 1)

-----------------------------------Email using my way: Get wwork email and home email to note
, WorkEmail as (select REFERENCE,replace(EMAIL_ADD,',','.') EMAIL_ADD
from PROP_EMAIL TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID 
where CONFIG_NAME = 'work' and EMAIL_ADD like '%_@_%.__%')

, homeEmail as (select REFERENCE,replace(EMAIL_ADD,',','.') EMAIL_ADD
from PROP_EMAIL TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID 
where CONFIG_NAME = 'home' and EMAIL_ADD like '%_@_%.__%')
-------------each contact has only 1 email so no need to combine

select --distinct
	concat('BB',ccc.CONTACT) as 'contactexternalId'
	, he.EMAIL_ADD as homeemail
    , telmobile.TEL_NUMBER mobile
    , telhome.TEL_NUMBER as homephone
from PROP_X_CLIENT_CON cc
left join contact ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on ccc.CONTACT = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on ccc.CONTACT = telmobile.REFERENCE --candidate-mobile
left join WorkEmail we on ccc.CONTACT = we.REFERENCE
left join homeEmail he on ccc.CONTACT = he.REFERENCE
where  he.EMAIL_ADD is not null or telmobile.TEL_NUMBER is not null or telhome.TEL_NUMBER is not null
