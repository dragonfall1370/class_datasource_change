
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
	        + Coalesce('Office Type: ' + NULLIF(cast(UC.customText4 as varchar(max)), '') + char(10), '')
	        + Coalesce('Location: ' + NULLIF(cast(UC.customText2 as varchar(max)), '') + char(10), '')
	        + Coalesce('Status: ' + NULLIF(cast(UC.status as varchar(max)), '') + char(10), '')
                + Coalesce('Reports To: ' + NULLIF(cast(UC1.name as varchar(max)), '') + char(10), '') --reportToUserID
                
	        --+ case when CL.isDeleted = 1 then concat('Contact is deleted: ',char(10)) else '' end
                 /*concat('Phone: ',ltrim(Stuff( Coalesce(' ' + NULLIF(UC.phone, ''), '')
                                + Coalesce(', ' + NULLIF(UC.phone2, ''), '')
                                + Coalesce(', ' + NULLIF(UC.phone3, ''), '')
                                --+ Coalesce(', ' + NULLIF(UC.mobile, ''), '')
                                --+ Coalesce(', ' + NULLIF(UC.workPhone, ''), '')
                                , 1, 1, '') ), char(10)) */
	        --+ Coalesce('Other Phone: ' + NULLIF(UC.Phone2, '') + char(10), '')
                
                
                --+ Coalesce('Reports to: ' + NULLIF(cast(UC.reportToUserID as varchar(max)), '') + char(10), '')
                + Coalesce('Other Phone: ' + NULLIF(cast(UC.phone2 as varchar(max)), '') + char(10), '')
                --+ Coalesce('Fax: ' + NULLIF(cast(UC.fax as varchar(max)), '') + char(10), '')
                --+ Coalesce('Office: ' + NULLIF(cast(UC.office as varchar(max)), '') + char(10), '')
                + Coalesce('Address 1: ' + NULLIF(cast(UC.address1 as varchar(max)), '') + char(10), '')
                + Coalesce('Address 2: ' + NULLIF(cast(UC.address2 as varchar(max)), '') + char(10), '')
                + Coalesce('City: ' + NULLIF(cast(UC.city as varchar(max)), '') + char(10), '')
                + Coalesce('State: ' + NULLIF(cast(UC.state as varchar(max)), '') + char(10), '')
                + Coalesce('Country: ' + NULLIF(cast(UC.countryID as varchar(max)), '') + char(10), '')
                + Coalesce('Post Code: ' + NULLIF(cast(UC.zip as varchar(max)), '') + char(10), '')
                + Coalesce('Postal Address 1: ' + NULLIF(cast(UC.customText5 as varchar(max)), '') + char(10), '')
                + Coalesce('Postal Address 2: ' + NULLIF(cast(UC.customText10 as varchar(max)), '') + char(10), '')
                + Coalesce('Postal City: ' + NULLIF(cast(UC.customText11 as varchar(max)), '') + char(10), '')
                + Coalesce('Postal State: ' + NULLIF(cast(UC.customText12 as varchar(max)), '') + char(10), '')
                + Coalesce('Postal Country: ' + NULLIF(cast(UC.customText14 as varchar(max)), '') + char(10), '')
                + Coalesce('Postal Post Code: ' + NULLIF(cast(UC.customText13 as varchar(max)), '') + char(10), '')
                --+ Coalesce('Referred by: ' + NULLIF(cast(UC.referredByUserID as varchar(max)), '') + char(10), '')
                + Coalesce('Referred By: ' + NULLIF(cast(UC2.name as varchar(max)), '') + char(10), '') --referredByUserID
                --+ Coalesce('General Comments: ' + NULLIF(cast(UC.comments as varchar(max)), '') + char(10), '')
                + Coalesce('General Comments: ' + NULLIF(cast(Cl.Comments as varchar(max)), '') + char(10), '')
                + Coalesce('Ideal Candidates to Float: ' + NULLIF(cast(UC.customTextBlock1 as varchar(max)), '') + char(10), '')
                --+ Coalesce('Custom Component 1: ' + NULLIF(cast(UC.CustomComponent1 as varchar(max)), '') + char(10), '')
                --+ Coalesce('Custom Component 2: ' + NULLIF(cast(UC.CustomComponent2 as varchar(max)), '') + char(10), '')
                --+ Coalesce('Custom Component 3: ' + NULLIF(cast(UC.CustomComponent3 as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Work Phone: ' + NULLIF(UC.WorkPhone, '') + char(10), '')
	        /*
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
		
		
		
		+ Coalesce('Recruiter User ID: ' + NULLIF(cast(Cl.recruiterUserID as varchar(max)), '') + char(10), '')
		
                + Coalesce('Type: ' + NULLIF(cast(Cl.type as varchar(max)), '') + char(10), '')
                
                + Coalesce('Employment Preference: ' + NULLIF(employmentPreference, '') + char(10), '')
                + Coalesce('Status: ' + NULLIF(Cl.status, '') + char(10), '')
                
		+ Coalesce('Designed Categories: ' + NULLIF(cast(Cl.desiredCategories as varchar(max)), '') + char(10), '')
		+ Coalesce('Desired Specialties: ' + NULLIF(cast(Cl.desiredSpecialties as varchar(max)), '') + char(10), '')
		+ Coalesce('Desired Skills: ' + NULLIF(cast(Cl.desiredSkills as varchar(max)), '') + char(10), '')
		--+ Coalesce('Lead Status: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
		--+ Coalesce('Reason Lost: ' + NULLIF(customText12, '') + char(10), '')
		
		--+ Coalesce('Company Previously Worked at: ' + NULLIF(customText15, '') + char(10), '')
		--+ Coalesce('Sold By: ' + NULLIF(customText11, '') + char(10), '')
		
		+ Coalesce('Primary Candidates Looking For: ' + NULLIF(customText4, '') + char(10), '')
		+ Coalesce('Secondary Candidates Looking For: ' + NULLIF(customText5, '') + char(10), '')
		+ Coalesce('Tertiary Candidates Looking For: ' + NULLIF(customText13, '') + char(10), '')
		+ Coalesce('Company Size: ' + NULLIF(customText18, '') + char(10), '')
		+ Coalesce('Industry: ' + NULLIF(customText14, '') + char(10), '')
		
		--+ Coalesce('Last Note: ' + NULLIF( replace(replace(replace(replace(replace(replace([dbo].[udf_StripHTML](UC.lastNote_denormalized),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"'),'&gt;','') , '') + char(10), '') --cast(UC.lastNote_denormalized as varchar(max))
                + Coalesce('Last Note: ' + NULLIF(ltrim(rtrim([dbo].[udf_StripHTML]( 
                        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                                   cast(UC.lastNote_denormalized as varchar(max))
                       ,'&nbsp;','') ,'&ndash;','') ,'&amp;',''), '&hellip;','') ,'&#39;','') ,'&gt;','') ,'&lt;','') ,'&quot;','') ,'&rsquo;',''), '&ldquo;',''), '&rdquo;','') ,'&reg;','') ,'&euro;','')  ) )), '') + char(10), '')
                */
                , 1, 0, '') as note
        -- select top 50 * -- select *
        from bullhorn1.BH_UserContact UC --where name like '%Andy Teng%'
        left join tmp_country on UC.countryID = tmp_country.CODE
        left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
        left join (select userID,name from bullhorn1.BH_UserContact) UC1 on UC.reportToUserID = UC1.userID       
        left join (select userID,name from bullhorn1.BH_UserContact) UC2 on UC.referredByUserID = UC2.userID
        --left join (select userID,name from bullhorn1.BH_UserContact) UC2 on Cl.recruiterUserID = UC2.userID
        --left join (select userID,name from bullhorn1.BH_UserContact) UC3 on UC.reportToUserID = UC3.userID
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
                left join bullhorn1.BH_User U on UC.commentingUserID = U.userID /* order by U.dateAdded desc */ )

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
	, UC.NickName as 'PreferredName' --<<
	, UC2.email as 'contact-owners'
	, UC2.name as '#Owners Name'
	
	, UC.Phone as 'Contact-Phone'
	--, UC.Phone2 as 'Contact-WorkPhone', UC.Phone3 as 'Contact-WorkPhone'
	, UC.Mobile as 'Contact-MobilePhone'
	--, ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone, ''), ''), 1, 0, '') ) as 'contact-phone'
        /*, ltrim(Stuff( Coalesce(NULLIF(UC.Phone, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.Phone2, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.Phone3, ''), '')
                         + Coalesce('' + NULLIF(UC.Mobile, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.WorkPhone, ''), '')
                , 1, 0, '') ) as 'contact-phone'  */
	--, e1.email as 'contact-email'
	--, UC.fax as 'contact-skype'
        , iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), e1.email) as 'contact-email'
	, UC.occupation as 'contact-jobTitle'
	, doc.files as 'contact-document'
	, note.note as 'contact-note'
        --, replace(replace(replace(replace(replace(ltrim(rtrim([dbo].[udf_StripHTML](c.comments))),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'contact-comment'
	--, len(replace(c.comments,'&#x0D;','')) as '(length-contact-comment)'
-- select count(*) --7487 -- select top 10 * -- select UC.Phone,UC. Phone2, UC. Phone3
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



/*
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

/* Group comments by ContactID (option 2)
, y as ( SELECT userID
        ,concat(concat('Date added: ',convert(varchar(10),dateAdded,120),' || ')
        ,iif(action = '' or action is null,'',concat('ACTION: ',action,' || '))
        ,concat('Comments: ',CONVERT(NVARCHAR(MAX), comments))
        ) as  comments
        FROM bullhorn1.BH_UserComment )

, x as ( SELECT userID, comments, r1 = ROW_NUMBER() OVER (PARTITION BY userID ORDER BY comments) FROM y)
, a AS (  SELECT userID, comments, r1 FROM x WHERE r1 = 1)
, r AS (  SELECT userID, comments, r1 FROM a WHERE r1 = 1 UNION ALL SELECT x.userID, r.comments + char(10) + x.comments, x.r1 FROM x INNER JOIN r ON r.userID = x.userID AND x.r1 = r.r1 + 1 )
--SELECT userID, comments = MAX(comments) FROM r GROUP BY userID ORDER BY userID OPTION (MAXRECURSION 0)

, comments (userID, comments) as (SELECT userID, comments = MAX(comments) FROM r GROUP BY userID)
select top 10 *,len(comments) as '(length-contact-comment)' from comments
*/

/*
with t as (
select    UC.clientCorporationID as 'contact-companyId'
	, Cl.clientID as 'contact-externalId'
	, concat(
	       case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end --as 'contact-firstName'
	   ,' '  , case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end --as 'contact-Lastname'
	) as fullname
	, UC.NickName as 'PreferredName'
        --, UC.Phone as 'Contact-WorkPhone'
	, ltrim(Stuff(  Coalesce(' ' + NULLIF(UC.Phone, '') + ' (Main)', '')
	               + Coalesce(', ' + NULLIF(UC.Phone2, '') + ' (Direct)', '')
                , 1, 1, '') ) as 'contact-phone' 
	,UC.Mobile as 'Contact-MobilePhone'
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
where isPrimaryOwner = 1 )
select count(*) from t

*/
