--ALTER DATABASE [DatabaseName] SET COMPATIBILITY_LEVEL = 130
with
  mail1 (ID,email) as (select Cl.clientID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email 
         from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
         left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
         where (Cl.isdeleted <> 1 and Cl.status <> 'Archive') )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--select * from ed


-- Secondary OWNER
-- select userID, ownerUserIDList FROM bullhorn1.BH_UserContact where ownerUserIDList is not null and ownerUserIDList like '%,%'
, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owner2b as (select owner2a.userid, UC.name from owner2a left join (select userid, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
--, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
, owner2c (userID,name) as (SELECT userID, STRING_AGG( name,',' ) WITHIN GROUP (ORDER BY name) name from owner2b GROUP BY userID)
--select * from owner2c where userid in (20983,13255)

-- ALL OWNER
/*with 
owners0 (userid, name) as (
       select Cl.userid,  STRING_AGG( concat(Cl.recruiterUserID,',',UC.ownerUserIDList),',' ) WITHIN GROUP (ORDER BY recruiterUserID) name 
       from bullhorn1.BH_Client Cl 
       left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
       where Cl.recruiterUserID is not null and UC.ownerUserIDList is not null and UC.ownerUserIDList like '%,%'
       GROUP BY Cl.userID )
, owners1 as (SELECT userID, Split.a.value('.', 'VARCHAR(200)') AS name FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),name),',','</M><M>') + '</M>' AS XML) AS Data FROM owners0) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owners2 as (select owners1.userid, UC.email, UC.name from owners1 left join (select userid, email, name from bullhorn1.BH_UserContact) UC on UC.userid = owners1.name where owners1.name <> '')
, owners3 (userID,name) as (SELECT userID, STRING_AGG( email,',' ) WITHIN GROUP (ORDER BY email) email from owners2 where email like '%_@_%.__%' GROUP BY userID)
select * from owners3 where userid in (20983,13255)
*/

------------
-- DOCUMENT
------------
/*, doc(clientContactUserID, files) as (
        SELECT    clientContactUserID
                , STUFF((SELECT DISTINCT ',' + concat(clientContactFileID,fileExtension) from bullhorn1.View_ClientContactFile WHERE clientContactUserID = a.clientContactUserID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html','.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS files
        FROM (select clientContactUserID from bullhorn1.View_ClientContactFile) AS a GROUP BY a.clientContactUserID ) */
, doc (clientContactUserID,files) as ( SELECT clientContactUserID, STRING_AGG(cast(concat(clientContactFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY clientContactFileID) att from bullhorn1.View_ClientContactFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ GROUP BY clientContactUserID )
--select * from doc where clientContactUserID in (63,86)


------------
-- NOTE
------------
, note as (
	select Cl.clientID
	, Stuff(  
	  Coalesce('BH Contact ID: ' + NULLIF(cast(UC.userID as varchar(max)), '') + char(10), '')
       + Coalesce('Email 3: ' + NULLIF(e3.email, '') + char(10), '')
       --+ Coalesce('General Comments: ' + NULLIF(cast(UC.comments as varchar(max)), '') + char(10), '')
       + Coalesce('Source: ' + NULLIF(cast(UC.source as varchar(max)), '') + char(10), '')
       + Coalesce('Status: ' + NULLIF(cast(UC.status as varchar(max)), '') + char(10), '')
       + Coalesce('General Contact Comments: ' + NULLIF(cast(Cl.Comments as varchar(max)), '') + char(10), '')                
--       + Coalesce('Fax: ' + NULLIF(cast(UC.Fax as varchar(max)), '') + char(10), '')
--       + Coalesce('Referred By: ' + NULLIF(cast(ref.name as varchar(max)), '') + char(10), '')
--       + Coalesce('Reports to: ' + NULLIF(cast(UC1.name as varchar(max)), '') + char(10), '')
--       + Coalesce('Secondary Owners: ' + NULLIF(cast(owner2c.name as varchar(max)), '') + char(10), '')
--       + Coalesce('Type: ' + NULLIF(cast(UC.Type as varchar(max)), '') + char(10), '')
--       + Coalesce('Type: ' + NULLIF(cast(Cl.type as varchar(max)), '') + char(10), '')
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
--       + Coalesce('Referred By UserID: ' + NULLIF(cast(referredByUserID as varchar(max)), '') + char(10), '')
--       + Coalesce('Referred By: ' + NULLIF(cast(referredBy as varchar(max)), '') + char(10), '')
--       + Coalesce('Date Last Visit: ' + NULLIF(cast(dateLastVisit as varchar(max)), '') + char(10), '')
--       + Coalesce('General Contact Comments: ' + NULLIF(cast(Cl.Comments as varchar(max)), '') + char(10), '')
--       + Coalesce('Recruiter User ID: ' + NULLIF(cast(Cl.recruiterUserID as varchar(max)), '') + char(10), '')
--       + Coalesce('Type: ' + NULLIF(cast(Cl.type as varchar(max)), '') + char(10), '')
--       + Coalesce('Employment Preference: ' + NULLIF(employmentPreference, '') + char(10), '')
--       + Coalesce('Status: ' + NULLIF(Cl.status, '') + char(10), '')
--       + Coalesce('Designed Categories: ' + NULLIF(cast(Cl.desiredCategories as varchar(max)), '') + char(10), '')
--       + Coalesce('Desired Specialties: ' + NULLIF(cast(Cl.desiredSpecialties as varchar(max)), '') + char(10), '')
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
        -- select top 10 * -- select *
        from bullhorn1.BH_UserContact UC --where name like '%Andy Teng%'
        left join tmp_country on UC.countryID = tmp_country.CODE
        left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
        left join (select userID,name from bullhorn1.BH_UserContact) UC1 on UC.reportToUserID = UC1.userID               
        left join (select userID,name from bullhorn1.BH_UserContact) UC2 on Cl.recruiterUserID = UC2.userID
        --left join e2 on Cl.userID = e2.ID
        left join e3 on Cl.userID = e3.ID
        left join (select userid, name from bullhorn1.BH_UserContact) ref on ref.userid = UC.referredByUserID
        left join owner2c on owner2c.userid = UC.userid
        --where Cl.isPrimaryOwner = 1 and Cl.isDeleted = 0
        )
--select type,recruiterUserID from bullhorn1.BH_Client
--select count(*) from note --10011
--select * from note




-----MAIN SCRIPT------
select top 3
         Cl.clientID as 'contact-externalId' --, Cl.userID as '#UserID'
       , iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation), convert(varchar(max),UC.clientCorporationID), 'default' ) as 'contact-companyId' --, UC.clientCorporationID as 'contact-companyId'
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
       , UC.middleName as 'contact-middleName'
       , case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , UC2.email as 'contact-owners' --, UC2.name as '#Owners Name'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone, ''), '') , 1, 1, '') ) as 'contact-phone' --+ Coalesce(', ' + NULLIF(UC.Phone2, ''), '') + Coalesce(', ' + NULLIF(UC.Phone3, ''), '') + Coalesce('' + NULLIF(UC.Mobile, ''), '') + Coalesce(', ' + NULLIF(UC.WorkPhone, ''), '')
       --, UC.fax as 'contact-skype'
       , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'contact-email'
       , UC.occupation as 'contact-jobTitle'
       , doc.files as 'contact-document'
       , note.note as 'contact-note'
       , Cl.dateadded as 'registration date'
       , UC.address1
       , UC.address2
       , UC.city
       , UC.state
       , UC.zip
       , UC.countryID
       , UC.Mobile as 'mobile_phone'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '') + Coalesce(', ' + NULLIF(UC.Phone3, ''), '') , 1, 1, '') ) as 'contact-home_phone' --, UC.Phone2 as 'home_phone' --, ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '') , 1, 0, '') ) as 'contact-home_phone'
       , Cl.division as 'Department'
       , e2.email as 'Personal Email'
       , UC.namePrefix as 'Tile'
       , UC.NickName as 'PreferredName'
       , Cl.desiredSkills as 'Skils'
-- select count(*) --7487 -- select top 10 *
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
left join ed ON Cl.clientID = ed.ID -- candidate-email-DUPLICATION
left join e2 ON Cl.clientID = e2.ID -- candidate-email
--left join e3 ON Cl.clientID = e3.ID -- candidate-email
--left join e4 ON Cl.clientID = e4.ID -- candidate-email
left join note on Cl.clientID = note.clientID
left join doc on Cl.userID = doc.clientContactUserID
where (Cl.isdeleted <> 1 and Cl.status <> 'Archive') --where isPrimaryOwner = 1 --and UC.clientCorporationID = 1284
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
