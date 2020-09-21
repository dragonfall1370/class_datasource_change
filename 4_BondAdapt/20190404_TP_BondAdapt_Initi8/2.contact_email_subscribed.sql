with
  contact0 (CLIENT,name,CONTACT,rn) as (SELECT cc.CLIENT, cg.name, cc.CONTACT,ROW_NUMBER() OVER(PARTITION BY CONTACT ORDER BY cc.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cc left join PROP_CLIENT_GEN cg on cg.reference = cc.client )
, contact as (select CLIENT, name, CONTACT from contact0 where rn = 1)
--select count(*) from contact --15702
--select * from contact where CONTACT = 38077

-- select REFERENCE, EMAIL_ADD,* from PROP_EMAIL where EMAIL_ADD like '%xuan%'

-- EMAIL
, mail1 (ID,email) as (select REFERENCE, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_ADD
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from PROP_EMAIL where EMAIL_ADD like '%_@_%.__%' )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 (ID,email) as (select ID, email from mail4 where rn = 4)
--select * from ed where ID in (45315)

, t as (
       select --top 500
                ccc.CONTACT as 'contact-externalId', pg.person_id
              , replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
              , case when (replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
              , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'contact-email'
              , pg.e_shot as 'Mail Subscribed'
              , 0 as 'subscribed'
       --select count(*) --15780--select count(distinct cc.CONTACT) --15701 rows -- select * from contact
       from contact ccc
       left join (
              select
                     pg.REFERENCE, pg.person_id, pg.FIRST_NAME, pg.LAST_NAME, cg.E_SHOT
              from PROP_PERSON_GEN pg
              left join PROP_CONT_GEN cg on cg.reference = pg.reference
              where cg.E_SHOT = 'N'
              ) pg on ccc.CONTACT = pg.REFERENCE
       left join ed ON ccc.CONTACT = ed.id -- DUPLICATED-EMAIL
       where pg.REFERENCE is not null and ed.email is not null

UNION
       select --top 500
                ccc.CONTACT as 'contact-externalId', pg.person_id
              , replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
              , case when (replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
              , e2.email as 'personal_email'
              , pg.e_shot as 'Mail Subscribed'
              , 0 as 'subscribed'
       --select count(*) --15780--select count(distinct cc.CONTACT) --15701 rows -- select * from contact
       from contact ccc
       left join (
              select
                     pg.REFERENCE, pg.person_id, pg.FIRST_NAME, pg.LAST_NAME, cg.E_SHOT
              from PROP_PERSON_GEN pg
              left join PROP_CONT_GEN cg on cg.reference = pg.reference
              where cg.E_SHOT = 'N'
              ) pg on ccc.CONTACT = pg.REFERENCE
       left join e2 ON ccc.CONTACT = e2.id -- Other-EMAIL  
       where pg.REFERENCE is not null and e2.email is not null
)       

--select count(*) from t
select *, 'imported' as 'tmp_mark' from t
