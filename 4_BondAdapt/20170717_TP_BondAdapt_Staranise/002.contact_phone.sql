with tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ',' + ltrim(rtrim(replace(TEL_NUMBER,',',' '))) from PROP_TELEPHONE  TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME != 'Mobile' and CONFIG_NAME != 'Home' and TEL_NUMBER != '' and TEL_NUMBER is not null and TEL_NUMBER <> '' and REFERENCE = a.REFERENCE FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
--select * from tel where REFERENCE = 395895

select --top 300
  	  ccc.CONTACT as 'contact-externalId'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when ( replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	, tel.TEL_NUMBER as 'contact-phone'
        , telmobile.TEL_NUMBER as mobile_phone
        , telhome.TEL_NUMBER as home_phone
        --, telwork.TEL_NUMBER as phone
 --select count(*)
from PROP_X_CLIENT_CON cc
left join (select CLIENT,CONTACT from (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg) c where c.rn = 1) ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE,FIRST_NAME,LAST_NAME, MIDDLE_NAME, JOB_TITLE, LINKED_IN,chifullname,chinesename from PROP_PERSON_GEN) pg on ccc.CONTACT = pg.REFERENCE
left join tel ON ccc.CONTACT = tel.REFERENCE
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on ccc.CONTACT = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on ccc.CONTACT = telmobile.REFERENCE --candidate-mobile
--left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work') telwork on ccc.CONTACT = telwork.REFERENCE --candidate-phone & candidate-workPhone
--where ccc.CONTACT = 395895