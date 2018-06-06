/*

with
------------
-- MAIL
------------
--contactmail as (select userID, concat(iif(email like '%_@_%.__%',concat(email,','),''),iif(email2 like '%_@_%.__%',concat(email2,','),''),iif(email3 like '%_@_%.__%',email3,'')) as email from bullhorn1.BH_UserContact)
--, combinedmail as (select userID, iif(right(email,1)=',',left(email,len(email)-1),email) as combinedmail from contactmail)
--select * from combinedmail
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact UC left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID where Cl.isPrimaryOwner = 1)
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
-- select * from ed where rn > 2 email like '%@%@%'


------------
-- NOTE
------------
, note as (
	select UC.userID
	, Stuff(  Coalesce('BH Contact ID: ' + NULLIF(cast(Cl.ClientID as varchar(max)), '') + char(10), '')
	        + Coalesce('Email: ' + NULLIF(email2, '') + char(10), '')
	        --+ case when CL.isDeleted = 1 then concat('Contact is deleted: ',char(10)) else '' end
                 concat('Phone: ',ltrim(Stuff( Coalesce(' ' + NULLIF(UC.phone, ''), '')
                                + Coalesce(', ' + NULLIF(UC.phone2, ''), '')
                                + Coalesce(', ' + NULLIF(UC.phone3, ''), '')
                                --+ Coalesce(', ' + NULLIF(UC.mobile, ''), '')
                                --+ Coalesce(', ' + NULLIF(UC.workPhone, ''), '')
                                , 1, 1, '') ), char(10))
	        
	        --+ Coalesce('Phone: ' + NULLIF(UC.Phone, '') + char(10), '')
	        + Coalesce('Work Phone: ' + NULLIF(UC.WorkPhone, '') + char(10), '')
	          Coalesce('Reports To: ' + NULLIF(cast(UC.reportToUserID as varchar(max)), '') + ' - ' + UC3.name + char(10), '')
	        + Coalesce('Email 2: ' + NULLIF(e2.email, '') + char(10), '')
	        + Coalesce('Email 3: ' + NULLIF(e3.email, '') + char(10), '')
	        + Coalesce('Department: ' + NULLIF(Cl.division, '') + char(10), '')
	        --+ Coalesce('BH Contact Owners: ' + NULLIF(UC2.name, '') + char(10), '')
	        + Coalesce('Address 1: ' + NULLIF(cast(address1 as varchar(max)), '') + char(10), '')
                + Coalesce('City: ' + NULLIF(city, '') + char(10), '')
                + Coalesce('State: ' + NULLIF(state, '') + char(10), '')
                + Coalesce('ZIP Code: ' + NULLIF(zip, '') + char(10), '')
                + Coalesce('Country: ' + NULLIF(tmp_country.COUNTRY, '') + char(10), '')
                + Coalesce('Source: ' + NULLIF(source, '') + char(10), '')
                + Coalesce('Referred By UserID: ' + NULLIF(cast(referredByUserID as varchar(max)), '') + char(10), '')
                + Coalesce('Referred By: ' + NULLIF(cast(referredBy as varchar(max)), '') + char(10), '')
		+ Coalesce('Date Last Visit: ' + NULLIF(cast(dateLastVisit as varchar(max)), '') + char(10), '')
		+ Coalesce('General Contact Comments: ' + NULLIF(cast(Cl.Comments as varchar(max)), '') + char(10), '')
        , 1, 0, '') as note
        -- select top 10 * -- select *
        from bullhorn1.BH_UserContact UC --where name like '%Andy Teng%'
        left join tmp_country on UC.countryID = tmp_country.CODE
        left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
        left join (select userID,name from bullhorn1.BH_UserContact) UC2 on Cl.recruiterUserID = UC2.userID
        left join (select userID,name from bullhorn1.BH_UserContact) UC3 on UC.reportToUserID = UC3.userID
        left join e2 on Cl.userID = e2.ID
        left join e3 on Cl.userID = e3.ID
        where Cl.isPrimaryOwner = 1 and Cl.isDeleted = 0
        )
--select type,recruiterUserID from bullhorn1.BH_Client
--select count(*) from note --10011
--select * from note

------------
-- COMMENT
------------
, comments as ( select UC.userID
                        --, UC.commentingUserID
                        , Stuff(
                          Coalesce('Created Date: ' + NULLIF(convert(varchar,U.dateAdded,120), '') + char(10), '')
                        + Coalesce('Commented by: ' + NULLIF(U.name, '') + char(10), '')
                        + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                        + Coalesce('Comments: ' + NULLIF(cast(comments as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as comments
                from bullhorn1.BH_UserComment UC 
                left join bullhorn1.BH_User U on UC.commentingUserID = U.userID ) -- order by U.dateAdded desc  )

------------
-- DOCUMENT
------------
, doc(clientContactUserID, files) as (
        SELECT    clientContactUserID
                , STUFF((SELECT DISTINCT ',' + concat(clientContactFileID,fileExtension) from bullhorn1.View_ClientContactFile WHERE clientContactUserID = a.clientContactUserID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS files
        FROM (select clientContactUserID from bullhorn1.View_ClientContactFile) AS a GROUP BY a.clientContactUserID )


-----MAIN SCRIPT------
select    UC.clientCorporationID as 'contact-companyId'
	, Cl.clientID as 'contact-externalId'
	, Cl.userID as '#UserID'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
	, UC.middleName as 'contact-middleName'
	, UC.NickName as 'PreferredName'
	, UC2.email as 'contact-owners'
	, UC2.name as '#Owners Name'
	
	, UC.Phone as 'Contact-WorkPhone'
	, ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '')
                , 1, 0, '') ) as 'contact-phone'
	,UC.Mobile as 'Contact-MobilePhone'
        , ltrim(Stuff( Coalesce(NULLIF(UC.Phone, ''), '')
                        + Coalesce(', ' + NULLIF(UC.Phone2, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.Phone3, ''), '')
                         + Coalesce('' + NULLIF(UC.Mobile, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.WorkPhone, ''), '')
                , 1, 0, '') ) as 'contact-phone' 
	
	--, e1.email as 'contact-email'
	, UC.fax as 'contact-skype'
        , iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), e1.email) as 'contact-email'
	, UC.occupation as 'contact-jobTitle'
	, doc.files as 'contact-document'
	, note.note as 'contact-note'
        --, replace(replace(replace(replace(replace(ltrim(rtrim([dbo].[udf_StripHTML](c.comments))),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'contact-comment'
	--, len(replace(c.comments,'&#x0D;','')) as '(length-contact-comment)'
-- select count(*) --7487 -- select top 10 *
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
--left join mail5 ON Cl.userID = mail5.ID
left join e1 ON Cl.userID = e1.ID -- candidate-email
left join e2 ON Cl.userID = e2.ID -- candidate-email
--left join e3 ON Cl.userID = e3.ID -- candidate-email
--left join e4 ON Cl.userID = e4.ID -- candidate-email
left join ed ON Cl.userID = ed.ID -- candidate-email-DUPLICATION


--left join comments c on Cl.userID = c.userID
left join note on Cl.userID = note.userID
left join doc on Cl.userID = doc.clientContactUserID
where isPrimaryOwner = 1 --and UC.clientCorporationID = 1284
--and concat (UC.FirstName,' ',UC.LastName) like '%Partha%'
--and Cl.clientID in (3007,8,7,123,76,163)
--order by Cl.clientID desc




------------
-- COMMENT - INJECT TO VINCERE
CREATE SEQUENCE seq_contact START WITH 10000 INCREMENT BY 1;
alter table contact_comment alter column id set default nextval('seq_contact');
commit;
select id,external_id,first_name,last_name from contact where external_id in ('2076','2083','1812','2037','2904','531','2439')
select * from contact_comment where contact_id = 50908
INSERT INTO contact_comment (contact_id, user_id, comment_content, insert_timestamp) VALUES ( 64167, -10, 'TESTING', '2019-01-01 00:00:00' )

------------
with comments as ( select Cl.clientID as 'contact_id' ,concat(UC1.firstName,' ',UC1.lastName) as fullname
                        --, UC.userID 
                        , cast('-10' as int) as user_account_id
                        , U.dateAdded as insert_timestamp
                        , 'comment' as category
                        , 'contact' as type
                        , Stuff(  Coalesce('Created Date: ' + NULLIF(convert(varchar,U.dateAdded,120), '') + char(10), '')
                                + Coalesce('Commented by: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(cast(UC.comments as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as content
                -- select top 100 *
                from bullhorn1.BH_UserComment UC
                left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID
                left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
                left join bullhorn1.BH_User U on U.userID = UC.commentingUserID )
--select * from comments where contact_id is not null and contact_id in (4054,7102) --538216 > 563579
select count(*) from comments where contact_id is not null and contact_id in (4054,7102)

-- OLD
with comments as ( select Cl.clientID as 'externalId' ,concat(UC1.firstName,' ',UC1.lastName) as fullname
                        --, UC.userID 
                        , cast('-10' as int) as userid
                        , U.dateAdded as insert_timestamp
                        , Stuff(  Coalesce('Created Date: ' + NULLIF(convert(varchar,U.dateAdded,120), '') + char(10), '')
                                + Coalesce('Commented by: ' + NULLIF(U.name, '') + char(10), '')
                                + Coalesce('Action: ' + NULLIF(UC.action, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(cast(UC.comments as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') as comment_content
                -- select top 10 *
                from bullhorn1.BH_UserComment UC
                left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID
                left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
                left join bullhorn1.BH_User U on U.userID = UC.commentingUserID )
select top 10 * from comments --538216 > 563579
select count(*) from comments where externalId is not null --and externalId = 29112


*/


with
------------
-- MAIL
------------
--contactmail as (select userID, concat(iif(email like '%_@_%.__%',concat(email,','),''),iif(email2 like '%_@_%.__%',concat(email2,','),''),iif(email3 like '%_@_%.__%',email3,'')) as email from bullhorn1.BH_UserContact)
--, combinedmail as (select userID, iif(right(email,1)=',',left(email,len(email)-1),email) as combinedmail from contactmail)
--select * from combinedmail
  mail1 (ID,email) as (select UC.ID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from Contact UC )
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
        -- select count(*) --182
        from Attachment at
        left join Contact a on a.id = at.ParentId
        where (at.name like '%doc' or at.name like '%docx' or at.name like '%pdf' or at.name like '%rtf' or at.name like '%xls' or at.name like '%xlsx')
        and a.id is not null
         )
, doc (ParentId, docs) as (SELECT ParentId, STUFF((SELECT ', ' + doc from doc0 WHERE doc0.ParentId <> '' and ParentId = a.ParentId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs FROM doc0 as a where a.ParentId <> '' GROUP BY a.ParentId)
--select * from doc-
         
, note as (
	select c.ID
	, Stuff( Coalesce('ID: ' + NULLIF(cast(c.ID as varchar(max)), '') + char(10), '')
                        + Coalesce('Current Status: ' + NULLIF(c.Current_status__c, '') + char(10), '')
                        + Coalesce('Next Step: ' + NULLIF(c.Next_action_Fr__c, '') + char(10), '')
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
        , 'franck@consultingpositions.net' as 'contact-owners' --, c.OwnerId as 'contact-owners'
        , c.FirstName as 'contact-firstName'
        , c.LastName as 'contact-Lastname'
        , c.AccountId  as 'contact-companyId'
        , c.Salutation as "Title"
        , c.Title as 'contact-jobTitle'
        , c.LinkedIn_Profile__c as "LinkedIn"
        --, c.Status_Fr__c as 'contact-owners' -- EMPTY
        --, c.Phone as 'contact-phone' --as "Contact Primary Phone"
        --, c.Phone_work__c as 'Contact-WorkPhone' --as "Contact Primary Phone"
        , ltrim(Stuff(    Coalesce(', ' + NULLIF(c.Phone, ''), '')
                        + Coalesce(', ' + NULLIF(c.Phone_work__c, ''), '')
                , 1, 1, '') ) as 'contact-phone' 
        , c.MobilePhone as "MobilePhone"

        --, c.Email as 'contact-email'
        , iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), e1.email) as 'contact-email'
        , c.Second_e_mail__c as "PersonalEmail"
        , c.HomePhone as 'homePhone'
        , c.Birthdate as 'dob'
        , note.note as 'contact-note'
        , doc.docs as 'contact-document'
-- select distinct Birthdate --Salutation -- Status_Fr__c --OwnerId -- select count(*)
from Contact c --where AccountId is null or AccountId = ''
left join Account a on a.id = c.AccountId
left join note on note.id = c.id
left join doc on doc.ParentId = c.id
left join e1 ON e1.ID = c.ID -- candidate-email
left join ed ON ed.ID = c.ID -- candidate-email-DUPLICATION
--where c.id in ('0031A000028IYXdQAO','0038000000hLWG7AAO','0031A000028IYW6QAO','0030y00002DM0YLAA1','0031A000027YeQ3QAK','003C0000016wztcIAA','0031A000027aKqsQAE','0031A000027aSlaQAE','0031A000027Y95EQAS','0031A000024e5tNQAQ','0031A0000218ZhmQAE','0031A000025Wi9oQAC','0031A000025XAytQAG','0031A00002AMFbfQAH','0031A000028IYXiQAO','0038000000hLWG8AAO','0031A000028IYWBQA4','0031A000028IH97QAG','0031A000025XK4UQAW','0038000000ls8uhAAA','0031A000027bNngQAE','0031A000025PdHcQAK','0031A000025Wi9tQAC','0031A000028LwJMQA0','003C000001KLqqXIAT','0031A000021AtDWQA0','0038000000oTqmjAAC','0031A000028IQblQAG')
--where a.id is null



/*
-- COMMENT
with t as (
        select    c.ID as 'externalId'
                , CONVERT(datetime,convert(varchar(50),c.CreatedDate,120)) as 'insert_timestamp'
                , cast('-10' as int) as 'user_account_id'
                , 'comment' as 'category'
                , 'contact' as 'type'
                --, c.Comments_FJ_Fr__c, c.Comments_base__c
                , ltrim(Stuff(   Coalesce('Comments_Fr__c: ' + NULLIF(c.Comments_Fr__c, '') + char(10), '')
                               + Coalesce('Comments_FNJ_Fr__c: ' + NULLIF(c.Comments_FNJ_Fr__c, '') + char(10), '')
                        , 1, 0, '')) as 'content'        
        from Contact c
        )
--select count(*) from t where content is not null
select * from t where content is not null

*/

/*
with t as (
        select    c.ID as 'contact-externalId'
                , CONVERT(datetime,convert(varchar(50),c.CreatedDate,120)) as 'insert_timestamp'
                , cast('-10' as int) as 'user_account_id'
                , 'comment' as 'category'
                , 'contact' as 'type'
                , ltrim(Stuff(   Coalesce('Subject: ' + NULLIF(e.Subject, '') + char(10), '')
                               + Coalesce('Location: ' + NULLIF(e.Location, '') + char(10), '')
                               + Coalesce('Duration In Minutes: ' + NULLIF(e.DurationInMinutes, '') + char(10), '')
                               + Coalesce('Description: ' + NULLIF(e.Description, '') + char(10), '')
                               + Coalesce('Show As: ' + NULLIF(e.ShowAs, '') + char(10), '')
                               + Coalesce('Reminder Date Time: ' + NULLIF(e.ReminderDateTime, '') + char(10), '')
                               + Coalesce('Proposed Event Timeframe: ' + NULLIF(e.ProposedEventTimeframe, '') + char(10), '')
                        , 1, 0, '')) as 'content'
        from Contact c
        left join Event e on e.whoid = c.id where e.whoid is not null
        --left join Event e on e.whatid = c.id where e.whatid is not null
        )
select * from t where content is not null

*/