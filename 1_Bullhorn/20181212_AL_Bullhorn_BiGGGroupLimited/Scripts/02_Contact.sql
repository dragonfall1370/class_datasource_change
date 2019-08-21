drop table if exists VCCons


-- ALTER DATABASE [ro2] SET COMPATIBILITY_LEVEL = 130 ;
;with
--  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact UC left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID where Cl.isPrimaryOwner = 1)
--, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
--, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
--, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
--, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
--, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
--, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
--, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--select * from ed

SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT cl.userid, CAST('<M>' + REPLACE(cast(UC.skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x from bullhorn1.BH_UserContact UC left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID where Cl.isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SkillName0 as a where a.skillID <> '' GROUP BY a.userId)

-- Secondary OWNER
, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owner2b as (select  owner2a.userid, UC.name from owner2a left join (select userid, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
--select * from owner2c where userid in (8281,12389,6467,10883,4281)
------------
-- NOTE
------------
, note as (
	select Cl.clientID --UC.userID
	
	, Stuff(
		Coalesce('Contact ID: ' + NULLIF(cast(UC.userID as varchar(max)), '') + char(10), '')
        
		+ Coalesce('General Comments: ' + NULLIF(cast(Cl.comments as varchar(max)), '') + char(10), '')

		+ Coalesce('Brand: ' + NULLIF(cast(UC.customTextBlock1 as varchar(max)), '') + char(10), '')

		+ Coalesce('Status: ' + NULLIF(cast(UC.status as varchar(max)), '') + char(10), '')

        --+ Coalesce('Fax: ' + NULLIF(cast(UC.fax as varchar(max)), '') + char(10), '')

		--+ concat('Enrolled in NPS: ', iif(npsui.isEnrolled = 0, 'No', 'Yes'), char(10))

		--+ concat('NPS Opt Out: ', iif(npsui.isOptedOut = 0, 'No', 'Yes'), char(10))

		--+ concat('Referred By: ', UC.referredByUserID, char(10))

		--+ concat('Reports to: ', UC.reportToUserID, char(10))

		--+ Coalesce('Referred by: ' + NULLIF(cast(ref.name as varchar(max)), '') + char(10), '')

        --+ Coalesce('Reports to: ' + NULLIF(cast(UC1.name as varchar(max)), '') + char(10), '')

		--+ Coalesce('Type: ' + NULLIF(cast(Cl.type as varchar(max)), '') + char(10), '')

                
        --+ Coalesce('Address 1: ' + NULLIF(cast(address1 as varchar(max)), '') + char(10), '')
        --+ Coalesce('City: ' + NULLIF(city, '') + char(10), '')
        --+ Coalesce('State: ' + NULLIF(state, '') + char(10), '')
        --+ Coalesce('ZIP Code: ' + NULLIF(zip, '') + char(10), '')
        --+ Coalesce('Country: ' + NULLIF(VC_Countries.COUNTRY, '') + char(10), '')
        --+ Coalesce('Enrolled in NPS: ' + NULLIF(cast(Cl.npsIsEnrolled as varchar(max)), '') + char(10), '')
        --+ Coalesce('Secondary Owners: ' + NULLIF(cast(owner2c.name as varchar(max)), '') + char(10), '')
        --+ Coalesce('Skills: ' + NULLIF(cast(SN.SkillName as varchar(max)), '') + char(10), '')
        --+ Coalesce('Skills: ' + NULLIF(cast(UC.skillSet as varchar(max)), '') + char(10), '')
        --+ Coalesce('General Contact Comments: ' + NULLIF(cast(Cl.Comments as varchar(max)), '') + char(10), '')                
		--+ Coalesce('Fax: ' + NULLIF(cast(UC.Fax as varchar(max)), '') + char(10), '')
        
		, 1, 0, ''
	) as note 
    -- select top 10 * -- select revenue
    from bullhorn1.BH_UserContact UC -- name like '%Andy Teng%'
    left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID --where Cl.Type is not null
	left join Bullhorn1.BH_NPSUserInfo npsui on UC.UserID = npsui.UserID
    left join VC_Countries on UC.countryID = VC_Countries.CODE
    left join (select userid, name from bullhorn1.BH_UserContact) ref on ref.userid = UC.referredByUserID
    left join (select userID,name from bullhorn1.BH_UserContact) UC1 on UC.reportToUserID = UC1.userID               
    left join (select userID,name from bullhorn1.BH_UserContact) UC2 on Cl.recruiterUserID = UC2.userID
    --left join e2 on Cl.userID = e2.ID
    --left join e3 on Cl.userID = e3.ID
	--left join VCConEmails ce on ce.UserID = Cl.userID
    left join SkillName SN on SN.userId = cl.userID
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
                , STUFF((SELECT DISTINCT ',' + concat(clientContactFileID,fileExtension) from bullhorn1.View_ClientContactFile WHERE clientContactUserID = a.clientContactUserID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS files
        FROM (select clientContactUserID from bullhorn1.View_ClientContactFile) AS a GROUP BY a.clientContactUserID )


-----MAIN SCRIPT------
select
    Cl.userID as 'UserID'
	, Cl.clientID as 'contact-externalId'
    , UC.clientCorporationID as 'contact-companyId'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
	, [dbo].[ufn_TrimSpecialCharacters_V2](UC.middleName, '') as 'contact-middleName'
	, lower([dbo].[ufn_TrimSpecialCharacters_V2](UC2.email, '')) as 'contact-owners' --, UC2.name as '#Owners Name'
	, [dbo].[ufn_RefinePhoneNumber_V2](UC.Phone) as 'contact-phone'
	, [dbo].[ufn_RefinePhoneNumber_V2](UC.Phone2) as 'contact-homePhone' --<<
	--, ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '') , 1, 0, '') ) as 'contact-phone'
	, [dbo].[ufn_RefinePhoneNumber_V2](UC.Mobile) as 'Contact-MobilePhone'
/*       , ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone, ''), '')
                         + Coalesce(', ' + NULLIF(UC.Phone2, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.Phone3, ''), '')
                         --+ Coalesce('' + NULLIF(UC.Mobile, ''), '')
                        --+ Coalesce(', ' + NULLIF(UC.WorkPhone, ''), '')
                , 1, 1, '') ) as 'contact-phone' */
	--, UC.fax as 'contact-skype'
    --, iif(ed.rn > 1,concat(ed.rn, '-', ed.email), ed.email) as 'contact-email'
	, ces.Email as 'contact-email'
	, [dbo].[ufn_TrimSpecialCharacters_V2](UC.occupation, '') as 'contact-jobTitle'
	, isnull(doc.files, '') as 'contact-document'
	, isnull(note.note, '') as 'contact-note'
        --, replace(replace(replace(replace(replace(ltrim(rtrim([dbo].[udf_StripHTML](c.comments))),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'contact-comment'
	--, len(replace(c.comments,'&#x0D;','')) as '(length-contact-comment)'
    --, UC.customText12 as 'contact-LinkedIn'
	, UC.address1 --<<
    , UC.address2 --<<
    , UC.city --<<
    , UC.state --<<
    , UC.zip --<<
    , UC.countryID --<<
    , UC.namePrefix -->> Tile
	, UC.NickName -->> 'PreferredName'
    , Cl.division --<< Department
    --, e2.email --<< Personal Email
	, isnull(NULLIF(cast(UC.skillSet as varchar(max)), ''), '') as Skills
-- select count(*) --7487 -- select UC.customText1

into VCCons

from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
--left join mail5 ON Cl.userID = mail5.ID
--left join e1 ON Cl.userID = e1.ID -- candidate-email
--left join e2 ON Cl.userID = e2.ID -- candidate-email
--left join e3 ON Cl.userID = e3.ID -- candidate-email
--left join e4 ON Cl.userID = e4.ID -- candidate-email
--left join ed ON Cl.userID = ed.ID -- candidate-email-DUPLICATION
--left join comments c on Cl.userID = c.userID
left join dbo.VCConEmails ces on Cl.userID = ces.userID
left join note on Cl.clientID = note.clientID
left join doc on Cl.userID = doc.clientContactUserID
where isPrimaryOwner = 1 --and UC.clientCorporationID = 1284
--and concat (UC.FirstName,' ',UC.LastName) like '%Partha%'
--and Cl.clientID in (3007,8,7,123,76,163)
--order by Cl.clientID desc



/*
select
       Cl.clientID as 'contact-externalId', UC.userID
       , Stuff(
                                   Coalesce('' + NULLIF(UC.address1, ''), '')
                               + Coalesce(', ' + NULLIF(UC.address2, ''), '')
                               + Coalesce(', ' + NULLIF(UC.city, ''), '')
                               + Coalesce(', ' + NULLIF(UC.state, ''), '')
                               + Coalesce(', ' + NULLIF(tc.country, ''), '')
                       , 1, 1, '') as 'address'
       , UC.city
       , UC.state
       , UC.zip as 'post_code'
       , tc.ABBREVIATION as 'country_code' --UC.countryID
       , ltrim(Stuff(
                         Coalesce(', ' + NULLIF(UC.city, ''), '')
                        + Coalesce(', ' + NULLIF(UC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 1, '') ) as 'locationName'
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join VC_Countries tc ON UC.countryID = tc.code
where isPrimaryOwner = 1
and Cl.clientID = 999

-- select * from VC_Countries

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

select
[contact-companyId]
, [contact-externalId]
, [contact-lastName]
, [contact-middleName]
, [contact-firstName]
--, [contact-firstNameKana]
--, [contact-lastNameKana]
, [contact-email]
, [contact-phone]
, [contact-jobTitle]
, [contact-Note]
, [contact-document]
--, [contact-photo]
--, [contact-linkedin]
--, [contact-skype]
, [contact-owners]
from VCCons
--where [contact-email] like '%john.garratt@leicester.gov.uk'
--where [contact-companyId] not in (select [company-externalId] from VCComs)
--where len(trim(isnull([contact-Lastname], ''))) = 0