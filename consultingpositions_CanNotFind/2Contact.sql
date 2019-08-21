--ALTER DATABASE [consultingpositions2] SET COMPATIBILITY_LEVEL = 130

with
------------
-- MAIL
------------
--contactmail as (select userID, concat(iif(email like '%_@_%.__%',concat(email,','),''),iif(email2 like '%_@_%.__%',concat(email2,','),''),iif(email3 like '%_@_%.__%',email3,'')) as email from bullhorn1.BH_UserContact)
--, combinedmail as (select userID, iif(right(email,1)=',',left(email,len(email)-1),email) as combinedmail from contactmail)
--select * from combinedmail
  mail1 (ID,email) as (select UC.ID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from Contact UC )
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
        -- select count(*) --182
        from Attachment at
        left join Contact a on a.id = at.ParentId
        --where (at.name like '%doc' or at.name like '%docx' or at.name like '%pdf' or at.name like '%rtf' or at.name like '%xls' or at.name like '%xlsx')
        and a.id is not null
         )
, doc (ParentId, docs) as (SELECT ParentId, STUFF((SELECT ', ' + doc from doc0 WHERE doc0.ParentId <> '' and ParentId = a.ParentId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs FROM doc0 as a where a.ParentId <> '' GROUP BY a.ParentId)
, taskdoc (whoid, docs) as (SELECT whoid, STUFF((SELECT ', ' + truong_att from task WHERE task.truong_att  is not null and task.truong_att <> '' and whoid = a.whoid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs FROM task as a where a.truong_att  is not null and a.truong_att <> '' GROUP BY a.whoid)
--select top 200 * from taskdoc
         
, note as (
	select c.ID
	, Stuff( Coalesce('ID: ' + NULLIF(cast(c.ID as varchar(max)), '') + char(10), '')
                        + Coalesce('Current Status: ' + NULLIF(c.Current_status__c, '') + char(10), '')
                        + Coalesce('Next Step: ' + NULLIF(c.Next_action_Fr__c, '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(c.Status_Fr__c, '') + char(10), '')
                        + Coalesce('Priority Status: ' + NULLIF(c.Priority__c, '') + char(10), '')
                        + Coalesce('1st LinkedIn Degree Contact Of: ' + NULLIF(c.X1st_LinkedIn_degree_contact_of__c, '') + char(10), '')
                        /*+ 'Mailing Address: '  + char(10)
                        + Coalesce('Mailing Street: ' + NULLIF(c.MailingStreet, '') + char(10), '')
                        + Coalesce('Mailing City: ' + NULLIF(c.MailingCity, '') + char(10), '')
                        + Coalesce('Mailing State: ' + NULLIF(c.MailingState, '') + char(10), '')
                        + Coalesce('Mailing PostalCode: ' + NULLIF(c.MailingPostalCode, '') + char(10), '')
                        + Coalesce('Mailing Country: ' + NULLIF(c.MailingCountry, '') + char(10), '') */
                        + Coalesce('Mailing Address: '  + char(10) + NULLIF(
                                           Coalesce('Mailing Street: ' + NULLIF(c.MailingStreet, '') + char(10), '')
                                        + Coalesce('Mailing City: ' + NULLIF(c.MailingCity, '') + char(10), '')
                                        + Coalesce('Mailing State: ' + NULLIF(c.MailingState, '') + char(10), '')
                                        + Coalesce('Mailing PostalCode: ' + NULLIF(c.MailingPostalCode, '') + char(10), '')
                                        + Coalesce('Mailing Country: ' + NULLIF(c.MailingCountry, '') + char(10), '')            
                        , '') + char(10), '')
                        /*+ 'Other Address: '  + char(10)
                        + Coalesce('Other Street: ' + NULLIF(c.OtherStreet, '') + char(10), '')
                        + Coalesce('Other City: ' + NULLIF(c.OtherCity, '') + char(10), '')
                        + Coalesce('Other State: ' + NULLIF(c.OtherState, '') + char(10), '')
                        + Coalesce('Other PostalCode: ' + NULLIF(c.OtherPostalCode, '') + char(10), '')
                        + Coalesce('Other Country: ' + NULLIF(c.OtherCountry, '') + char(10), '') */
                        + Coalesce('Other Address: ' + char(10) + NULLIF(
                                           Coalesce('Other Street: ' + NULLIF(c.OtherStreet, '') + char(10), '')
                                        + Coalesce('Other City: ' + NULLIF(c.OtherCity, '') + char(10), '')
                                        + Coalesce('Other State: ' + NULLIF(c.OtherState, '') + char(10), '')
                                        + Coalesce('Other PostalCode: ' + NULLIF(c.OtherPostalCode, '') + char(10), '')
                                        + Coalesce('Other Country: ' + NULLIF(c.OtherCountry, '') + char(10), '')
                           , '') + char(10), '')
                , 1, 0, '') as note
                -- select  top 10 *
        from Contact c
        )
--select * from note --where note like '%&%;%'

select    c.ID as 'contact-externalId'
        , u.username as 'contact-owners' --, 'franck@consultingpositions.net' as 'contact-owners'
        , c.FirstName as 'contact-firstName'
        , c.LastName as 'contact-Lastname'
        , c.AccountId  as 'contact-companyId'
        , c.Salutation as "Title" --<<<
        , c.Title as 'contact-jobTitle'
        , c.LinkedIn_Profile__c as "LinkedIn" --<<<
        , ltrim(Stuff(Coalesce(', ' + NULLIF(c.Phone, ''), '')
                        + Coalesce(', ' + NULLIF(c.Phone_work__c, ''), '')
                , 1, 1, '') ) as 'contact-phone' 
        , c.MobilePhone as "contact-MobilePhone" --<<<
        , iif(ed.rn > 1,concat(ed.email,'_',ed.rn), ed.email) as 'contact-email'
        , c.Second_e_mail__c as "PersonalEmail" --<<<
        , c.HomePhone as 'homePhone'
        , c.Birthdate as 'dob'
        , note.note as 'contact-note'
        , concat(doc.docs,',',taskdoc.docs) as 'contact-document'
-- select distinct Birthdate --Salutation -- Status_Fr__c --OwnerId -- select count(*)
from Contact c --where AccountId is null or AccountId = ''
left join Users u on u.ID = c.OwnerId
left join Account a on a.id = c.AccountId
left join note on note.id = c.id
left join doc on doc.ParentId = c.id
left join taskdoc on taskdoc.whoid = c.id
left join ed ON ed.ID = c.ID -- candidate-email-DUPLICATION
--where c.id in ('0031A000028IYXdQAO','0038000000hLWG7AAO','0031A000028IYW6QAO','0030y00002DM0YLAA1','0031A000027YeQ3QAK','003C0000016wztcIAA','0031A000027aKqsQAE','0031A000027aSlaQAE','0031A000027Y95EQAS','0031A000024e5tNQAQ','0031A0000218ZhmQAE','0031A000025Wi9oQAC','0031A000025XAytQAG','0031A00002AMFbfQAH','0031A000028IYXiQAO','0038000000hLWG8AAO','0031A000028IYWBQA4','0031A000028IH97QAG','0031A000025XK4UQAW','0038000000ls8uhAAA','0031A000027bNngQAE','0031A000025PdHcQAK','0031A000025Wi9tQAC','0031A000028LwJMQA0','003C000001KLqqXIAT','0031A000021AtDWQA0','0038000000oTqmjAAC','0031A000028IQblQAG')
--where a.id is null





with t as (
       -- COMMENT       
        select    c.ID as 'externalId'
                , CONVERT(datetime,convert(varchar(50),c.CreatedDate,120)) as 'comment_timestamp|insert_timestamp'
                , ltrim(Stuff(   Coalesce('Comments Franck: ' + NULLIF(c.Comments_Fr__c, '') + char(10), '')
                               + Coalesce('Comments Others: ' + NULLIF(c.Comments_FNJ_Fr__c, '') + char(10), '')
                        , 1, 0, '')) as 'content'        
        from Contact c
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
       left join Contact c on c.id = n.parentid
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
       left join Contact c on c.id = e.whoid
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u1 on u1.id = e.createdbyid
       left join (select id, concat(firstname,' ',lastname, ' - ', username) as name from users) u2 on u2.id = e.LastModifiedById
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
       left join Contact c on c.id = t.whoid
       where c.id is not null     
        )

--select count(*) from t where content is not null --25883
select --top 100
                    externalid as 'externalId'
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'contact' as 'type'
                  , [comment_timestamp|insert_timestamp] as 'comment_timestamp|insert_timestamp'
                  , content as 'content'
from t --where note <> '' 




