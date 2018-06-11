
with
  contact0 (CLIENT,CONTACT,rn) as (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg)

, contact as (select CLIENT,CONTACT from contact0 where rn = 1)

, title as (
	SELECT REFERENCE, MN.DESCRIPTION as TITLE 
	FROM PROP_PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_PERSON_GEN.TITLE 
	where MN.LANGUAGE=1)


, contactTitle as (select --distinct
	concat('BB',ccc.CONTACT) as 'contactexternalId'
	, concat('BB',ccc.CLIENT) as 'contact-companyId'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when ( replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	, case
		when t.TITLE like 'Sir' then 'MR'
		when t.TITLE like 'Doctor' then 'DR'
		else upper(t.TITLE) end as 'contactTitle'
from PROP_X_CLIENT_CON cc
left join contact ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE,FIRST_NAME,LAST_NAME, MIDDLE_NAME, JOB_TITLE, LINKEDIN, SALUTATION from PROP_PERSON_GEN) pg on ccc.CONTACT = pg.REFERENCE
left join title t on cc.CONTACT = t.REFERENCE)
select * from contactTitle where contacttitle is null