ALTER DATABASE [cormacconsulting] SET COMPATIBILITY_LEVEL = 130;

with
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact UC left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID where Cl.isPrimaryOwner = 1)
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--select * from ed

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
*/

-- Secondary OWNER
, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owner2b as (select  owner2a.userid, UC.name from owner2a left join (select userid, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
--select * from owner2c where userid in (8281,12389,6467,10883,4281)


------------
-- NOTE
------------
, note as (
	select UC.userID
	, Stuff(  
	    Coalesce('BH Contact ID: ' + NULLIF(cast(UC.userID as varchar(max)), '') + char(10), '')
       + Coalesce('Email 3: ' + NULLIF(e3.email, '') + char(10), '')
       + Coalesce('General Contact Comments: ' + NULLIF(cast(Cl.Comments as varchar(max)), '') + char(10), '')                
       --+ Coalesce('General Comments: ' + NULLIF(cast(UC.comments as varchar(max)), '') + char(10), '')
       + Coalesce('Fax: ' + NULLIF(cast(UC.Fax as varchar(max)), '') + char(10), '')
       + Coalesce('Title: ' + NULLIF(cast(UC.namePrefix as varchar(max)), '') + char(10), '')
       --+ Coalesce('Enrolled in NPS: ' + NULLIF(cast(UC.npsIsEnrolled as varchar(max)), '') + char(10), ''
       + Coalesce('Referred By: ' + NULLIF(cast(ref.name as varchar(max)), '') + char(10), '')
       + Coalesce('Reports to: ' + NULLIF(cast(UC1.name as varchar(max)), '') + char(10), '')       
       --+ Coalesce('revenue: ' + NULLIF(cast(UC.revenue as varchar(max)), '') + char(10), '')       
       + Coalesce('Desired Specialties: ' + NULLIF(cast(Cl.desiredSpecialties as varchar(max)), '') + char(10), '')
       + Coalesce('Status: ' + NULLIF(cast(UC.status as varchar(max)), '') + char(10), '') 
--       + Coalesce('Secondary Owners: ' + NULLIF(cast(owner2c.name as varchar(max)), '') + char(10), '')
       + Coalesce('Type: ' + NULLIF(cast(Cl.type as varchar(max)), '') + char(10), '')
--       + Coalesce('Email: ' + NULLIF(email2, '') + char(10), '')
--       + case when CL.isDeleted = 1 then concat('Contact is deleted: ',char(10)) else '' end
--       + concat('Phone: ',ltrim(Stuff( Coalesce(' ' + NULLIF(UC.phone, ''), '')
--                       + Coalesce(', ' + NULLIF(UC.phone2, ''), '')
--                       + Coalesce(', ' + NULLIF(UC.phone3, ''), '')
--                       --+ Coalesce(', ' + NULLIF(UC.mobile, ''), '')
--                       --+ Coalesce(', ' + NULLIF(UC.workPhone, ''), '')
--                       , 1, 1, '') ), char(10))
--       + Coalesce('Phone: ' + NULLIF(UC.Phone, '') + char(10), '')
--       + Coalesce('Work Phone: ' + NULLIF(UC.WorkPhone, '') + char(10), '')
--       + Coalesce('Reports To: ' + NULLIF(cast(UC.reportToUserID as varchar(max)), '') + ' - ' + UC3.name + char(10), '')
--       + Coalesce('Email 2: ' + NULLIF(e2.email, '') + char(10), '')
--       + Coalesce('Department: ' + NULLIF(Cl.division, '') + char(10), '')
--       + Coalesce('BH Contact Owners: ' + NULLIF(UC2.name, '') + char(10), '')
--       + Coalesce('Address 1: ' + NULLIF(cast(address1 as varchar(max)), '') + char(10), '')
--       + Coalesce('City: ' + NULLIF(city, '') + char(10), '')
--       + Coalesce('State: ' + NULLIF(state, '') + char(10), '')
--       + Coalesce('ZIP Code: ' + NULLIF(zip, '') + char(10), '')
--       + Coalesce('Country: ' + NULLIF(tmp_country.COUNTRY, '') + char(10), '')
--       + Coalesce('Source: ' + NULLIF(source, '') + char(10), '')
--       + Coalesce('Referred By UserID: ' + NULLIF(cast(referredByUserID as varchar(max)), '') + char(10), '')
--       + Coalesce('Referred By: ' + NULLIF(cast(referredBy as varchar(max)), '') + char(10), '')
--       + Coalesce('Date Last Visit: ' + NULLIF(cast(dateLastVisit as varchar(max)), '') + char(10), '')
--       + Coalesce('Recruiter User ID: ' + NULLIF(cast(Cl.recruiterUserID as varchar(max)), '') + char(10), '')
--       + Coalesce('Type: ' + NULLIF(cast(Cl.type as varchar(max)), '') + char(10), '')
--       + Coalesce('Employment Preference: ' + NULLIF(employmentPreference, '') + char(10), '')
--       + Coalesce('Status: ' + NULLIF(Cl.status, '') + char(10), '')
--       + Coalesce('Designed Categories: ' + NULLIF(cast(Cl.desiredCategories as varchar(max)), '') + char(10), '')

--       + Coalesce('Desired Skills: ' + NULLIF(cast(Cl.desiredSkills as varchar(max)), '') + char(10), '')
--       + Coalesce('Lead Status: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
--       + Coalesce('Reason Lost: ' + NULLIF(customText12, '') + char(10), '')
--       + Coalesce('Company Previously Worked at: ' + NULLIF(customText15, '') + char(10), '')
--       + Coalesce('Sold By: ' + NULLIF(customText11, '') + char(10), '')
--       + Coalesce('Primary Candidates Looking For: ' + NULLIF(customText4, '') + char(10), '')
--       + Coalesce('Secondary Candidates Looking For: ' + NULLIF(customText5, '') + char(10), '')
--       + Coalesce('Tertiary Candidates Looking For: ' + NULLIF(customText13, '') + char(10), '')
--       + Coalesce('Company Size: ' + NULLIF(customText18, '') + char(10), '')
--       + Coalesce('Industry: ' + NULLIF(customText14, '') + char(10), '')
--       + Coalesce('Last Note: ' + NULLIF( replace(replace(replace(replace(replace(replace([dbo].[udf_StripHTML](UC.lastNote_denormalized),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"'),'&gt;','') , '') + char(10), '') --cast(UC.lastNote_denormalized as varchar(max))
--       + Coalesce('Last Note: ' + NULLIF(ltrim(rtrim([dbo].[udf_StripHTML]( 
--               replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
--                          cast(UC.lastNote_denormalized as varchar(max))
--              ,'&nbsp;','') ,'&ndash;','') ,'&amp;',''), '&hellip;','') ,'&#39;','') ,'&gt;','') ,'&lt;','') ,'&quot;','') ,'&rsquo;',''), '&ldquo;',''), '&rdquo;','') ,'&reg;','') ,'&euro;','')  ) )), '') + char(10), '') */
       , 1, 0, '') as note 
        -- select top 10 * -- select UC.Type
        from bullhorn1.BH_UserContact UC --where name like '%Andy Teng%'
        left join tmp_country on UC.countryID = tmp_country.CODE
        left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
        left join (select userID,name from bullhorn1.BH_UserContact) UC1 on UC.reportToUserID = UC1.userID               
        left join (select userID,name from bullhorn1.BH_UserContact) UC2 on Cl.recruiterUserID = UC2.userID
        --left join e2 on Cl.userID = e2.ID
        left join e3 on Cl.userID = e3.ID
        left join (select userid, name from bullhorn1.BH_UserContact) ref on ref.userid = UC.referredByUserID
        left join owner2c on owner2c.userid = UC.userid
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
                , STUFF((SELECT DISTINCT ',' + concat(clientContactFileID,fileExtension) from bullhorn1.View_ClientContactFile WHERE clientContactUserID = a.clientContactUserID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS files
        FROM (select clientContactUserID from bullhorn1.View_ClientContactFile) AS a GROUP BY a.clientContactUserID )


-----MAIN SCRIPT------
select   
         Cl.clientID as 'contact-externalId' --, Cl.userID as '#UserID'
       , UC.clientCorporationID as 'contact-companyId'
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
       , case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , UC.middleName as 'contact-middleName'
       , UC2.email as 'contact-owners' --, UC2.name as '#Owners Name'
       --, UC.Phone as 'Contact-WorkPhone'
       --, ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '') , 1, 0, '') ) as 'contact-phone'
       , UC.Mobile as 'Contact-MobilePhone'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone, ''), '') + Coalesce(', ' + NULLIF(UC.Phone2, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.Phone3, ''), '') + Coalesce('' + NULLIF(UC.Mobile, ''), '') + Coalesce(', ' + NULLIF(UC.WorkPhone, ''), '')
                , 1, 1, '') ) as 'contact-phone'
       --, UC.fax as 'contact-skype'
       , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'contact-email'
       , UC.occupation as 'contact-jobTitle'
       , doc.files as 'contact-document'
       , note.note as 'contact-note'
       --, replace(replace(replace(replace(replace(ltrim(rtrim([dbo].[udf_StripHTML](c.comments))),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'contact-comment', len(replace(c.comments,'&#x0D;','')) as '(length-contact-comment)'
       , UC.address1
       , UC.address2
       , UC.city
       , UC.state
       , UC.zip
       , UC.countryID
       , Cl.division --> Department
       , e2.email --> Personal Email
       , UC.namePrefix --> Tile
       , UC.NickName as 'PreferredName'
       --, Cl.desiredCategories as Skills
-- select count(*) --7487 -- select 
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
