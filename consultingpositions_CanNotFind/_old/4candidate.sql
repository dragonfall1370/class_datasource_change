

with 

------------
-- MAIL
------------
--contactmail as (select userID, concat(iif(email like '%_@_%.__%',concat(email,','),''),iif(email2 like '%_@_%.__%',concat(email2,','),''),iif(email3 like '%_@_%.__%',email3,'')) as email from bullhorn1.BH_UserContact)
--, combinedmail as (select userID, iif(right(email,1)=',',left(email,len(email)-1),email) as combinedmail from contactmail)
--select * from combinedmail
  mail1 (ID,email) as (select UC.ID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from Lead UC )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
--, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--, ed0 (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID ASC) AS rn FROM mail4) --DUPLICATION
--, ed1 (ID,email,rn) as (select distinct ID,email,rn from ed0 where rn > 1)
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
, e3 as (select ID, email from mail4 where rn = 3)
--, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed where rn > 2 email like '%@%@%'


, doc0 as ( select at.ParentId, concat(at.id,'_',replace(at.Name,',','') ) as doc, concat(a.FirstName,' ', a.LastName) as fullname
        -- select count(*) --4549
        from Attachment at
        left join Lead a on a.id = at.ParentId
        where (at.name like '%doc' or at.name like '%docx' or at.name like '%pdf' or at.name like '%rtf' or at.name like '%xls' or at.name like '%xlsx')
        and a.id is not null
         )
, doc (ParentId, docs) as (SELECT ParentId, STUFF((SELECT ', ' + doc from doc0 WHERE doc0.ParentId <> '' and ParentId = a.ParentId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs FROM doc0 as a where a.ParentId <> '' GROUP BY a.ParentId)
--select * from doc

, note as (
	select l.ID
	, Stuff(Coalesce('ID: ' + NULLIF(cast(l.ID as varchar(max)), '') + char(10), '')
                + Coalesce('SF Lead Status: ' + NULLIF(l.Status, '') + char(10), '')
                + Coalesce('Experience: ' + NULLIF([dbo].[udf_StripHTML](l.Experience__c), '') + char(10), '')
                + Coalesce('Introduction: ' + NULLIF([dbo].[udf_StripHTML](l.Introduction__c), '') + char(10), '')
                + Coalesce('Sourced By: ' + NULLIF(l.Sourced_by__c, '') + char(10), '')
                + Coalesce('Reference CV: ' + NULLIF(l.Reference_CV__c, '') + char(10), '')
                + Coalesce('First Language: ' + NULLIF(l.First_language__c, '') + char(10), '')
                + Coalesce('Second Language: ' + NULLIF(l.Second_language__c, '') + char(10), '')
                + Coalesce('Other Languages: ' + NULLIF(l.Other_languages__c, '') + char(10), '')
                + Coalesce('Comments_on_languages__c: ' + NULLIF(l.Comments_on_languages__c, '') + char(10), '')
                + Coalesce('Nationality__c: ' + NULLIF(l.Nationality__c, '') + char(10), '')
                + Coalesce('Ex Company: ' + NULLIF(l.Ex_Company__c, '') + char(10), '')
                + Coalesce('Operational Consulting: ' + NULLIF(l.Operational_consulting__c, '') + char(10), '')
                + Coalesce('Source: ' + NULLIF(l.Source__c, '') + char(10), '')
                + Coalesce('Lead Source: ' + NULLIF(l.LeadSource, '') + char(10), '')
                , 1, 0, '') as note
                -- select Experience__c, Introduction__c, Sourced_by__c, Reference_CV__c, First_language__c, Second_language__c, Other_languages__c, Comments_on_languages__c, Nationality__c, Ex_Company__c, Operational_consulting__c, Brooks__c, Celerant__c, CPI__c, Highland__c, KMandT__c, Lausanne_Consulting__c, P_M__c, PDM__c, PiPint__c, ProAction__c, Quest_Worldwide__c, Resultance__c, SAMI__c, TA_Cook__c, Other_clients__c, CN_s_Clients__c, Billed_for__c, Source__c, LeadSource
        from Lead l
        )
--select count(*) from note where note is not null --like '%&%;%'
--select id,replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') from note where note is not null


, dup as (SELECT ID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(a.name)) ORDER BY a.ID ASC) AS rn FROM Account a where name like 'Sustainable')
--select * from dup
--select ID,name from dup where rn = (select max(rn) from dup )

, candidate_company as (
        select l.id, l.company as company -- , concat(l.FirstName,' ',l.LastName), l.Title__c
        from Lead l 
        where l.company <> concat(l.FirstName,' ',l.LastName) and l.ConvertedAccountId = '000000000000000AAA' 
UNION ALL
        select
                  l.id --, l.ConvertedAccountId
                , c.name  as company
        from Lead l
        left join Account c  on c.id = l.ConvertedAccountId 
        where c.name is not null and l.ConvertedAccountId <> '000000000000000AAA' 
)
-- select id from candidate_company group by id having count(*) >1


select --top 10
          l.ID as 'candidate-externalId'
        , Coalesce(NULLIF(replace(l.FirstName ,'?',''), ''), 'No Firstname') as 'candidate-firstName'
        , Coalesce(NULLIF(replace(l.LastName,'?',''), ''), concat('Lastname-',l.ID) ) as 'candidate-lastName'
        --, l.Company as 'Company' , l.ConvertedAccountId as 'candidate-company1'
        , cc.company as 'candidate-company1'
        , l.Title__c 'candidate-jobTitle1'
        , l.MobilePhone as 'candidate-mobile'
        --, l.Phone as 'Primary Phone'
        --, l.Other_phone__c as 'Primary Phone'
        , ltrim(Stuff(    Coalesce(', ' + NULLIF(l.Phone, ''), '')
                        + Coalesce(', ' + NULLIF(l.Other_phone__c, ''), '')
                , 1, 1, '')) as 'candidate-phone'
        --, l.Email as 'candidate-email'
        , iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), e1.email) as 'candidate-email'
        , l.Second_Email__c as 'candidate-workEmail' -->> 'Home Email'
        , l.Skype_ID__c as 'candidate-Skype'
        , replace(replace(replace(replace(replace([dbo].[udf_StripHTML](l.Chronology__c),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'Candidate-WorkHistory'
        --, l.Street City State PostalCode Country as 'Candidate Location Address = Street - City - State - Postal Code - Country  Candidate Location City, Candidate Location State, Candidate Location PostalCode, Candidate Location Country  Candidate Location Name = City - State - Country'
        , l.Website as 'Website'
        , replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'company-note'
        , doc.docs as 'candidate-resume'
-- select * -- select count(*)
from Lead l --4725
--left join (select ID,name from dup where rn = (select max(rn) from dup ) ) a on a.name = l.Company
left join note on note.ID = l.ID
left join doc on doc.ParentId = l.id
left join e1 ON e1.ID = l.ID -- candidate-email
left join ed ON ed.ID = l.ID -- candidate-email-DUPLICATION
left join candidate_company cc on cc.id = l.id
where l.id in ('00QC000001XWMxvMAH')
--where l.Company in ('LAgroup','ProConseil','Sustainable','You Improve')


/*
with note2 as (
	select l.ID
	       , 'Previously Sent Companies ' as title
	       , Stuff(Coalesce('Brooks: ' + NULLIF(l.Brooks__c, '') + char(10), '')
                + Coalesce('Celerant: ' + NULLIF(l.Celerant__c, '') + char(10), '')
                + Coalesce('CPI: ' + NULLIF(l.CPI__c, '') + char(10), '')
                + Coalesce('Highland: ' + NULLIF(l.Highland__c, '') + char(10), '')
                + Coalesce('KM and T: ' + NULLIF(l.KMandT__c, '') + char(10), '')
                + Coalesce('Lausanne_Consulting__c: ' + NULLIF(l.Lausanne_Consulting__c, '') + char(10), '')
                + Coalesce('P&M: ' + NULLIF(l.P_M__c, '') + char(10), '')
                + Coalesce('PDM: ' + NULLIF(l.PDM__c, '') + char(10), '')
                + Coalesce('PiPint: ' + NULLIF(l.PiPint__c, '') + char(10), '')
                + Coalesce('ProAction: ' + NULLIF(l.ProAction__c, '') + char(10), '')
                + Coalesce('Quest Worldwide: ' + NULLIF(l.Quest_Worldwide__c, '') + char(10), '')
                + Coalesce('Resultance: ' + NULLIF(l.Resultance__c, '') + char(10), '')
                + Coalesce('SAMI: ' + NULLIF(l.SAMI__c, '') + char(10), '')
                + Coalesce('TA Cook: ' + NULLIF(l.TA_Cook__c, '') + char(10), '')
                + Coalesce('Other Clients: ' + NULLIF(l.Other_clients__c, '') + char(10), '')
                + Coalesce('CN''s Clients: ' + NULLIF(l.CN_s_Clients__c, '') + char(10), '')
                + Coalesce('Billed For: ' + NULLIF(l.Billed_for__c, '') + char(10), '')
                , 1, 0, '') as note2
                -- select Experience__c, Introduction__c, Sourced_by__c, Reference_CV__c, First_language__c, Second_language__c, Other_languages__c, Comments_on_languages__c, Nationality__c, Ex_Company__c, Operational_consulting__c, Brooks__c, Celerant__c, CPI__c, Highland__c, KMandT__c, Lausanne_Consulting__c, P_M__c, PDM__c, PiPint__c, ProAction__c, Quest_Worldwide__c, Resultance__c, SAMI__c, TA_Cook__c, Other_clients__c, CN_s_Clients__c, Billed_for__c, Source__c, LeadSource
        from Lead l
        )
select count(*) from note2 where  note2 is not null
select * from note2 where note2 is not null
*/

/*
-- COMMENT
with t as (
        select    ID as 'externalId'
                , CONVERT(datetime,convert(varchar(50),l.CreatedDate,120)) as 'insert_timestamp0'
                , cast('-10' as int) as 'user_account_id'
                , 'comment' as 'category'
                , 'candidate' as 'type'
                , ltrim(Stuff(   Coalesce('Comments_Fr__c: ' + NULLIF(l.Comments_FJ_Fr__c, '') + char(10), '')
                               + Coalesce('Comments_FNJ_Fr__c: ' + NULLIF(l.Comments_FNJ_Fr__c, '') + char(10), '')
                        , 1, 0, '')) as 'content'
        from Lead l
        )
select count(*) from t where content is not null
select * from t where content is not null
*/
