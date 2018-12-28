
with
------------
-- MAIL
------------
--contactmail as (select userID, concat(iif(email like '%_@_%.__%',concat(email,','),''),iif(email2 like '%_@_%.__%',concat(email2,','),''),iif(email3 like '%_@_%.__%',email3,'')) as email from bullhorn1.BH_UserContact)
--, combinedmail as (select userID, iif(right(email,1)=',',left(email,len(email)-1),email) as combinedmail from contactmail)
--select * from combinedmail
  --mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact )
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact UC left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID where Cl.isPrimaryOwner = 1)
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
--, mail4 (ID,email,rn) as ( SELECT ID, email = CONVERT(NVARCHAR(MAX), email), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
--, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail3 where email like '%@%@%'
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
--, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--, ed0 (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID ASC) AS rn FROM mail4) --DUPLICATION
--, ed1 (ID,email,rn) as (select distinct ID,email,rn from ed0 where rn > 1)
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)

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
--select * from comments order by U.dateAdded desc  --where userID = 162517

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


------------
-- NOTE
------------
, note as (
	select UC.userID
	, Stuff(  Coalesce('BH Contact ID: ' + NULLIF(cast(Cl.ClientID as varchar(max)), '') + char(10), '')
	        + Coalesce('Email: ' + NULLIF(e2.email, '') + char(10), '')
	        --+ case when CL.isDeleted = 1 then concat('Contact is deleted: ',char(10)) else '' end
                + concat('Phone: ',ltrim(Stuff( Coalesce(' ' + NULLIF(UC.phone, ''), '')
                                + Coalesce(', ' + NULLIF(UC.phone2, ''), '')
                                + Coalesce(', ' + NULLIF(UC.phone3, ''), '')
                                --+ Coalesce(', ' + NULLIF(UC.mobile, ''), '')
                                + Coalesce(', ' + NULLIF(UC.workPhone, ''), '')
                                , 1, 1, '') ), char(10))
	        + Coalesce('Contact Owners: ' + NULLIF(UC2.name, '') + char(10), '')
	        + Coalesce('Address 1: ' + NULLIF(cast(address1 as varchar(max)), '') + char(10), '')
                + Coalesce('City: ' + NULLIF(city, '') + char(10), '')
                --+ Coalesce('State: ' + NULLIF(state, '') + char(10), '')
                + Coalesce('Country: ' + NULLIF(tmp_country.COUNTRY, '') + char(10), '')
                --+ Coalesce('Employment Preference: ' + NULLIF(employmentPreference, '') + char(10), '')
                + Coalesce('Status: ' + NULLIF(Cl.status, '') + char(10), '')
                --+ Coalesce('Department: ' + NULLIF(Cl.division, '') + char(10), '')
		+ Coalesce('Designed Categories: ' + NULLIF(cast(Cl.desiredCategories as varchar(max)), '') + char(10), '')
		--+ Coalesce('Desired Specialties: ' + NULLIF(cast(Cl.desiredSpecialties as varchar(max)), '') + char(10), '')
		+ Coalesce('Desired Skills: ' + NULLIF(cast(Cl.desiredSkills as varchar(max)), '') + char(10), '')
		--+ Coalesce('Lead Status: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
		+ Coalesce('Reason Lost: ' + NULLIF(customText12, '') + char(10), '')
		+ Coalesce('Source: ' + NULLIF(source, '') + char(10), '')
		+ Coalesce('Referred By: ' + NULLIF(cast(referredByUserID as varchar(max)), '') + char(10), '')
		+ Coalesce('Company Previously Worked at: ' + NULLIF(customText15, '') + char(10), '')
		--+ Coalesce('Sold By: ' + NULLIF(customText11, '') + char(10), '') -- CUSTOM FIELD
		+ Coalesce('Date Last Visit: ' + NULLIF(cast(dateLastVisit as varchar(max)), '') + char(10), '')
		+ Coalesce('Primary Candidates Looking For: ' + NULLIF(customText4, '') + char(10), '')
		+ Coalesce('Secondary Candidates Looking For: ' + NULLIF(customText5, '') + char(10), '')
		+ Coalesce('Tertiary Candidates Looking For: ' + NULLIF(customText13, '') + char(10), '')
		+ Coalesce('Company Size: ' + NULLIF(customText18, '') + char(10), '')
		+ Coalesce('Industry: ' + NULLIF(customText14, '') + char(10), '')
		+ Coalesce('Last Note: ' + NULLIF( replace(replace(replace(replace(replace(replace([dbo].[udf_StripHTML](UC.lastNote_denormalized),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"'),'&gt;','') , '') + char(10), '') --cast(UC.lastNote_denormalized as varchar(max))
                , 1, 0, '') as note
        -- select distinct status -- customText11 --customText3
        from bullhorn1.BH_UserContact UC
        left join tmp_country on UC.countryID = tmp_country.CODE
        left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
        left join e2 on Cl.userID = e2.ID
        left join (select userID,name from bullhorn1.BH_UserContact) UC2 on Cl.recruiterUserID = UC2.userID
        where Cl.isPrimaryOwner = 1 --and Cl.isDeleted = 0
        )
--select count(*) from note --81669 > 7630
--select * from note

------------
-- DOCUMENT
------------
, doc as (select a.clientContactUserID, concat(a.clientContactFileID,a.fileExtension) as contactFile from bullhorn1.View_ClientContactFile a where fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html'))
, contactdoc as (SELECT clientContactUserID, STUFF((SELECT DISTINCT ',' + contactFile from doc WHERE clientContactUserID = a.clientContactUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS contactFile FROM doc AS a GROUP BY a.clientContactUserID)


-----MAIN SCRIPT------
select  UC.clientCorporationID as 'contact-companyId'
	, Cl.clientID as 'contact-externalId'
	, Cl.userID as '#UserID'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'candidate-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'candidate-Lastname'
	, UC.middleName as '#contact-middleName'
	, UC2.email as 'contact-owners'
	, UC2.name as '#Owners Name'
	, Coalesce(NULLIF(UC.mobile, ''), '') as 'contact-phone'
	, iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), e1.email) as 'contact-email'
	, UC.occupation as 'contact-jobTitle'
	, contactdoc.contactFile as 'contact-document'
	, note.note as 'contact-Note'
        --, replace(replace(replace(replace(replace(ltrim(rtrim([dbo].[udf_StripHTML](c.comments))),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'contact-comment'
	--, len(replace(c.comments,'&#x0D;','')) as '(length-contact-comment)'
-- select count(*) --10078 -- select distinct status
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID

--left join mail5 ON Cl.userID = mail5.ID
left join e1 ON Cl.userID = e1.ID -- candidate-email
left join ed ON Cl.userID = ed.ID -- candidate-email-DUPLICATION

--left join comments c on Cl.userID = c.userID
left join note on Cl.userID = note.userID
left join contactdoc on Cl.userID = contactdoc.clientContactUserID
where isPrimaryOwner = 1
order by Cl.clientID desc

/*
select top 10 *
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
*/