

-- CONTACT WITHOUT COMPANY
with
-- EMAIL
  mail1 (ID,email) as (select ContactUniqueID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(email,',',HomeEmail),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from contacts where email like '%_@_%.__%' or HomeEmail like '%_@_%.__%' )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
--select * from ed where rn > 1

select --top 1000
          c.ContactUniqueID as 'candidate-externalid'
        , coalesce(nullif(c.Forename,''),'No FirstName') as 'candidate-firstName'
        , coalesce(nullif(c.Surname,''),'No LastName') as 'candidate-lastName'
        , coalesce( nullif(e1.email,''), coalesce( nullif(ed.email,'') + '(duplicated_' + cast(ed.rn as varchar(10)) + ')','') ) as 'candidate-email' --, c.Email as 'contact-email#'
        , can.*
-- select count(*)
from contacts c
left join e1 ON c.ContactUniqueID = e1.ID -- candidate-email
left join e2 ON c.ContactUniqueID = e2.ID
left join ed ON c.ContactUniqueID = ed.ID -- candidate-email-DUPLICATION
--and e1.email = 'varindaravej@gmail.com'
-- CANDIDATE
left join (
--with
--  mail (ID,email,rn) as ( SELECT UniqueID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY email ORDER BY email desc) FROM candidates where email <> '' and email like '%@%')
--, e1 as (select ID, email from mail where rn = 1)
--, ed (ID,email,rn) as (select ID, email, rn from mail where rn > 1)
select
          c.UniqueID As 'candidate-externalId'
        , coalesce(nullif(c.Forename,''),'No FirstName') As 'contact-firstName'
        , coalesce(nullif(c.Surname,''),'No LastName') As 'contact-lastName'
        , c.email as 'candidate-email'
        --, coalesce( nullif(e1.email,''), coalesce( nullif(ed.email,'') + '_duplicated' + cast(ed.rn as varchar(10)),'') ) As 'candidate-email' --, c.Email 
-- select count(*) --9198
from candidates c 
--left join e1 ON c.UniqueID = e1.ID -- candidate-email
--left join ed ON c.UniqueID = ed.ID -- candidate-email-DUPLICATION
--where e1.email = 'varindaravej@gmail.com'
--order by c.UniqueID asc
) can on can.[candidate-email] = e1.email
where c.SiteUnique = '0' --9583
and can.[candidate-email] is null
--and can.[candidate-email] is null