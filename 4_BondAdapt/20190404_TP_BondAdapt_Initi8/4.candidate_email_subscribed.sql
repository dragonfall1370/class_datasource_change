

with
-- tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ', ' + TEL_NUMBER from  PROP_TELEPHONE WHERE REFERENCE = a.REFERENCE FOR XML PATH ('')), 1, 2, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)

-- EMAIL
  mail1 (ID,email) as (select REFERENCE, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_ADD
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from PROP_EMAIL where EMAIL_ADD like '%_@_%.__%' )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 (ID,email) as (select ID, email from mail4 where rn = 4)
--select * from ed where ID in (45315)


, t as (
       select --top 1000
                pg.REFERENCE as 'candidate-externalId', pg.person_id
              , Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'contact-firstName'
              , Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'contact-lastName'
       
              , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'candidate-email' --, e1.email 
       
              , eshot.e_shot as 'Mail Subscribed'
              , 0 as 'subscribed'
              
       from PROP_PERSON_GEN pg
       left join ed ON ed.id = pg.REFERENCE -- DUPLICATED-EMAIL
       left join PROP_CAND_GEN eshot on eshot.reference = pg.reference
       where eshot.e_shot = 'N' and eshot.reference is not null and ed.email is not null

UNION

       select --top 1000
                pg.REFERENCE as 'candidate-externalId', pg.person_id
              , Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'contact-firstName'
              , Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'contact-lastName'
       
              , e2.email as 'candidate-workEmail'
       
              , eshot.e_shot as 'Mail Subscribed'
              , 0 as 'subscribed'
              
       from PROP_PERSON_GEN pg
       left join e2 ON e2.id = pg.REFERENCE -- Other-EMAIL
       left join PROP_CAND_GEN eshot on eshot.reference = pg.reference
       where eshot.e_shot = 'N' and eshot.reference is not null and e2.email is not null
)

--select count(*) from t
select *, 'imported' as 'tmp_mark' from t