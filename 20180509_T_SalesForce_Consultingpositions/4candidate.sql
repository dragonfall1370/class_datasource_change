

with 

------------
-- MAIL
------------
--contactmail as (select userID, concat(iif(email like '%_@_%.__%',concat(email,','),''),iif(email2 like '%_@_%.__%',concat(email2,','),''),iif(email3 like '%_@_%.__%',email3,'')) as email from bullhorn1.BH_UserContact)
--, combinedmail as (select userID, iif(right(email,1)=',',left(email,len(email)-1),email) as combinedmail from contactmail)
--select * from combinedmail
  mail1 (ID,email) as (select UC.ID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from Lead UC )
--, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
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
, taskdoc (whoid, docs) as (SELECT whoid, STUFF((SELECT ', ' + truong_att from task WHERE task.truong_att  is not null and task.truong_att <> '' and whoid = a.whoid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs FROM task as a where a.truong_att  is not null and a.truong_att <> '' GROUP BY a.whoid)
--select * from doc

, note as (
	select l.ID
	, Stuff(Coalesce('ID: ' + NULLIF(cast(l.ID as varchar(max)), '') + char(10), '')
                + Coalesce('SF Lead Status: ' + NULLIF(l.Status, '') + char(10), '')
                --+ Coalesce('Experience: ' + NULLIF([dbo].[fn_ConvertHTMLToText](l.Experience__c), '') + char(10), '') --<<<
                --+ Coalesce('Chronology: ' + NULLIF([dbo].[fn_ConvertHTMLToText](l.Chronology__c), '') + char(10), '') --<<<
                --+ Coalesce('Introduction: ' + NULLIF([dbo].[fn_ConvertHTMLToText](l.Introduction__c), '') + char(10), '') --<<<
                + Coalesce('Sourced by: ' + NULLIF(l.Sourced_by__c, '') + char(10), '') --<<
                + Coalesce('Reference CV: ' + NULLIF(l.Reference_CV__c, '') + char(10), '')
                + Coalesce('First Language: ' + NULLIF(l.First_language__c, '') + char(10), '')
                + Coalesce('Second Language: ' + NULLIF(l.Second_language__c, '') + char(10), '')
                + Coalesce('Other Languages: ' + NULLIF(l.Other_languages__c, '') + char(10), '')
                + Coalesce('Comments on languages: ' + NULLIF(l.Comments_on_languages__c, '') + char(10), '')
                + Coalesce('Nationality: ' + NULLIF(l.Nationality__c, '') + char(10), '')
                + Coalesce('Ex Company: ' + NULLIF(l.Ex_Company__c, '') + char(10), '')
                + Coalesce('Operational Consulting?: ' + NULLIF(l.Operational_consulting__c, '') + char(10), '')
                + Coalesce('Source: ' + NULLIF(l.Source__c, '') + char(10), '')
                + Coalesce('Lead Source: ' + NULLIF(l.LeadSource, '') + char(10), '')
                , 1, 0, '') as note
                -- select Operational_consulting__c
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
        --, l.MobilePhone as 'candidate-mobile'
        , ltrim(Stuff(    Coalesce(', ' + NULLIF(l.MobilePhone, ''), '')
                         + Coalesce(', ' + NULLIF(l.Phone, ''), '')
                        + Coalesce(', ' + NULLIF(l.Other_phone__c, ''), '')
                , 1, 1, '')) as 'candidate-phone'
        , iif(ed.rn > 1,concat(ed.email,'_',ed.rn), ed.email) as 'candidate-email'
        , l.Second_Email__c as 'candidate-workEmail' -->> 'Home Email'
        , l.Skype_ID__c as 'candidate-Skype'
        , u.username as 'candidate-owners' --, l.ownerid, l.sourced_by__c
        --, replace(replace(replace(replace(replace([dbo].[udf_StripHTML](l.Chronology__c),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'Candidate-WorkHistory'
        --,[dbo].[fn_ConvertHTMLToText](l.Chronology__c) as 'Candidate-WorkHistory'
		, l.city as 'candidate-city'
		, l.state as 'candidate-state'
              , l.postalcode as 'candidate-zipCode'
              , case
		when l.country like 'Africa%' then 'ZA'
		when l.country like 'Algeria%' then 'DZ'
		when l.country like 'America%' then 'US'
		when l.country like 'Argenti%' then 'AR'
		when l.country like 'Austral%' then 'AU'
		when l.country like 'Austria%' then 'AT'
		when l.country like 'Azerbai%' then 'AZ'
		when l.country like 'Belge%' then 'BE'
		when l.country like 'Belgiqu%' then 'BE'
		when l.country like 'Belgium%' then 'BE'
		when l.country like 'Belgiuy%' then 'BE'
		when l.country like 'BE%' then 'BE'
		when l.country like 'Bolivia%' then 'BO'
		when l.country like 'Bosnia%' then 'BA'
		when l.country like 'Botsawa%' then ''
		when l.country like 'Brasil%' then 'BR'
		when l.country like 'Brazil%' then 'BR'
		when l.country like 'Bulgari%' then 'BG'
		when l.country like 'Camerou%' then 'CM'
		when l.country like 'Canada%' then 'CA'
		when l.country like 'CA%' then 'CA'
		when l.country like 'Chile.%' then 'CL'
		when l.country like 'Chile%' then 'CL'
		when l.country like 'China%' then 'CN'
		when l.country like 'Colombi%' then 'US'
		when l.country like 'Croatia%' then 'HR'
		when l.country like 'Cyprus%' then 'CY'
		when l.country like 'Czech%' then 'CZ'
		when l.country like 'Czeck%' then 'CZ'
		when l.country like 'Denmark%' then 'DK'
		when l.country like 'Dubai%' then 'AE'
		when l.country like 'Ecuador%' then 'EC'
		when l.country like 'Egypt%' then 'EG'
		when l.country like 'Estonia%' then 'EE'
		when l.country like 'Finland%' then 'FI'
		when l.country like 'France%' then 'FR'
		when l.country like 'FR%' then 'FR'
		when l.country like 'Germany%' then 'DE'
		when l.country like 'Ghana%' then 'GH'
		when l.country like 'Greece%' then 'GR'
		when l.country like 'Guatema%' then 'GT'
		when l.country like 'Guinea%' then 'GN'
		when l.country like 'Holland%' then 'NL'
		when l.country like 'HongKon%' then 'HK'
		when l.country like 'Hong%' then 'HK'
		when l.country like 'Hungary%' then 'HU'
		when l.country like 'India%' then 'IN'
		when l.country like 'Indones%' then 'ID'
		when l.country like 'Iran%' then 'IR VE'
		when l.country like 'Ireland%' then 'IE'
		when l.country like 'Israel%' then 'IL'
		when l.country like 'ISR%' then 'IL'
		when l.country like 'Italy%' then 'IT'
		when l.country like 'Ivory%' then ''
		when l.country like 'Japan%' then 'JP'
		when l.country like 'Kazakhs%' then 'KZ'
		when l.country like 'Kenya%' then 'KE'
		when l.country like 'Korea%' then 'KR'
		when l.country like 'Kyrgyzs%' then 'KG'
		when l.country like 'Laos%' then 'LA'
		when l.country like 'Latvia%' then 'LV'
		when l.country like 'Lebanon%' then 'LB'
		when l.country like 'Lithuan%' then 'LT'
		when l.country like 'London%' then 'GB'
		when l.country like 'Luxembo%' then 'LU'
		when l.country like 'Malaysi%' then 'MY'
		when l.country like 'Marocco%' then 'MA'
		when l.country like 'Maroc%' then 'MA'
		when l.country like 'Maurita%' then 'MR'
		when l.country like 'México%' then 'MX'
		when l.country like 'Mexico%' then 'MX'
		when l.country like 'Monaco%' then 'MC'
		when l.country like 'Morocco%' then 'MA'
		when l.country like 'Myanmar%' then 'MM'
		when l.country like 'Netherl%' then 'NL'
		when l.country like 'Nigeria%' then 'NG'
		when l.country like 'Norway%' then 'NO'
		when l.country like 'Pakista%' then 'PK'
		when l.country like 'Peru%' then 'PE'
		when l.country like 'Philipp%' then 'PH'
		when l.country like 'Phillip%' then 'PH'
		when l.country like 'Poland%' then 'PL'
		when l.country like 'Portuga%' then 'PT'
		when l.country like 'Puerto%' then 'PR'
		when l.country like 'Qatar%' then 'QA'
		when l.country like 'Romania%' then 'RO'
		when l.country like 'Russia%' then 'RU'
		when l.country like 'Saudi%' then 'SA'
		when l.country like 'Serbia%' then ''
		when l.country like 'Singapo%' then 'SG'
		when l.country like 'Slovaki%' then 'SK'
		when l.country like 'Spain%' then 'ES'
		when l.country like 'Sweden%' then 'SE'
		when l.country like 'Swedwn%' then 'SE'
		when l.country like 'Switzer%' then 'CH'
		when l.country like 'Taiwan%' then 'TW'
		when l.country like 'Tanzani%' then 'TZ'
		when l.country like 'Thailan%' then 'TH'
		
		when l.country like 'Tunisia%' then 'TN'
		
		when l.country like 'Turkey%' then 'TR'
		when l.country like 'TX%' then 'US'
		when l.country like 'UAE%' then 'AE'
		when l.country like 'Ukraine%' then 'UA'
		when l.country like 'UK%' then 'GB'
		when l.country like 'Uruguay%' then 'UY'
		when l.country like 'Venezue%' then 'VE'
		when l.country like 'Vietnam%' then 'VN'
		when l.country like 'Zealand%' then 'NZ'
		when l.country like '%UNITED%ARAB%' then 'AE'
		when l.country like '%UAE%' then 'AE'
		when l.country like '%U.A.E%' then 'AE'
		when l.country like '%UNITED%KINGDOM%' then 'GB'
		when l.country like '%UNITED%STATES%' then 'US'
		when l.country like '%US%' then 'US'             
              end as 'candidate-Country'
              , ltrim(Stuff( Coalesce(' ' + NULLIF(l.street, ''), '')
                                + Coalesce(', ' + NULLIF(l.city, ''), '')
                                + Coalesce(', ' + NULLIF(l.state, ''), '')
                                + Coalesce(', ' + NULLIF(l.postalcode, ''), '')
                                + Coalesce(', ' + NULLIF(l.country, ''), '')
                                , 1, 1, '') ) as 'candidate-Address'        
        , l.Website as 'Website'
        --, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'candidate-note'
        , note.note as 'candidate-note'
        , concat(doc.docs,',',taskdoc.docs) as 'candidate-resume'
-- select top 100 * -- select count(*) --5022 -- select distinct country
from Lead l
left join Users u on u.ID = l.OwnerId
--left join (select ID,name from dup where rn = (select max(rn) from dup ) ) a on a.name = l.Company
left join note on note.ID = l.ID
left join doc on doc.ParentId = l.id
left join taskdoc on taskdoc.whoid = l.id
--left join e1 ON e1.ID = l.ID -- candidate-email
left join ed ON ed.ID = l.ID -- candidate-email-DUPLICATION
left join candidate_company cc on cc.id = l.id
where l.id in ('00QC000001XWMxvMAH')
--where l.Company in ('LAgroup','ProConseil','Sustainable','You Improve')




-- Candidate Note 2 "Previously Sent Companies "
with note2 as ( --
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


-------------------------------------
-- COMMENT



with t as (
       -- COMMENT       
        select    c.ID as 'externalId'
                , CONVERT(datetime,convert(varchar(50),c.CreatedDate,120)) as 'comment_timestamp|insert_timestamp'
                , ltrim(Stuff(   Coalesce('Comments Franck: ' + NULLIF(c.Comments_FJ_Fr__c, '') + char(10), '')
                               + Coalesce('Comments Others: ' + NULLIF(c.Comments_FNJ_Fr__c, '') + char(10), '')
                        , 1, 0, '')) as 'content'
        from Lead c
UNION ALL
       -- NOTE
       select  
                c.ID as 'externalId'
              , CONVERT(datetime,convert(varchar(50),n.CreatedDate,120)) as 'comment_timestamp|insert_timestamp'
              , Stuff( 'NOTE: ' + char(10)
                       + Coalesce('Title: ' + NULLIF(n.title, '') + char(10), '')
                       + Coalesce('Body: ' + NULLIF(n.body, '') + char(10), '')
                       + Coalesce('Created By: ' + NULLIF(u1.name, '') + char(10), '')
                       + Coalesce('Modified By:' + NULLIF(u2.name, '') + char(10), '')
                       + Coalesce('Modified Date: ' + NULLIF(n.LastModifiedDate, '') + char(10), '')
                       , 1, 0, '') as 'content'
              --, c.*, n.*
       from Note n
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u1 on u1.id = n.createdbyid
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u2 on u2.id = n.LastModifiedById
       left join lead c on c.id = n.parentid
       where c.id is not null
UNION ALL
       -- EVENT
       select  
                c.id as 'externalId'
              , CONVERT(datetime,convert(varchar(50),e.CreatedDate,120))  as 'comment_timestamp|insert_timestamp'
              , Stuff( 'EVENT: ' + char(10)
                       + Coalesce('Subject: ' + NULLIF(e.subject, '') + char(10), '')
                       + Coalesce('Location: ' + NULLIF(e.location, '') + char(10), '')
                       + Coalesce('Activity Date Time: ' + NULLIF(e.ActivityDateTime, '') + char(10), '')
                       + Coalesce('Activity Date: ' + NULLIF(e.ActivityDate, '') + char(10), '')
                       + Coalesce('Duration In Minutes: ' + NULLIF(e.DurationInMinutes, '') + char(10), '')
                       + Coalesce('Description: ' + NULLIF(e.Description, '') + char(10), '')
                       + Coalesce('Show As: ' + NULLIF(e.ShowAs, '') + char(10), '')
                       + Coalesce('Created By: ' + NULLIF(u1.name, '') + char(10), '')
                       + Coalesce('Modified By:' + NULLIF(u2.name, '') + char(10), '')
                       + Coalesce('Modified Date: ' + NULLIF(e.LastModifiedDate, '') + char(10), '')
                       + Coalesce('Reminder Date Time: ' + NULLIF(e.ReminderDateTime, '') + char(10), '')
                       + Coalesce('Proposed Event Timeframe: ' + NULLIF(e.ProposedEventTimeframe, '') + char(10), '')                
                       , 1, 0, '') as 'content'
              --, e.*
       -- select *
       from Event e
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u1 on u1.id = e.createdbyid
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u2 on u2.id = e.LastModifiedById
       left join lead c on c.id = e.whoid
       where c.id is not null
UNION ALL
       -- TASK
       select 
                c.id as 'externalId'
              , CONVERT(datetime,convert(varchar(50),t.CreatedDate,120)) as 'comment_timestamp|insert_timestamp'
              , Stuff( 'TASK: ' + char(10)
                       + Coalesce('Subject: ' + NULLIF(t.subject, '') + char(10), '')
                       + Coalesce('Status: ' + NULLIF(t.status, '') + char(10), '')
                       + Coalesce('Priority: ' + NULLIF(t.Priority, '') + char(10), '')
                       + Coalesce('Description: ' + NULLIF(replace(t.Description,'.  ',char(10)), '') + char(10), '')
                       + Coalesce('Created By: ' + NULLIF(u1.name, '') + char(10), '')
                       + Coalesce('Modified By:' + NULLIF(u2.name, '') + char(10), '')
                       + Coalesce('Modified Date: ' + NULLIF(t.LastModifiedDate, '') + char(10), '')
                       + Coalesce('Reminder Date Time: ' + NULLIF(t.ReminderDateTime, '') + char(10), '')
                       , 1, 0, '') as 'content'
       from task t
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u1 on u1.id = t.createdbyid
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u2 on u2.id = t.LastModifiedById
       left join lead c on c.id = t.whoid
       where c.id is not null     
        )

--select count(*) from t where content is not null --25883
select --top 100
                    externalid as 'externalId'
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'candidate' as 'type'
                  , [comment_timestamp|insert_timestamp] as 'comment_timestamp|insert_timestamp'
                  , content as 'content'
from t --where note <> '' 
