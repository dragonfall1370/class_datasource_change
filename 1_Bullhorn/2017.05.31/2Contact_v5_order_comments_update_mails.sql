
with
------------
-- MAIL
------------
--contactmail as (select userID, concat(iif(email like '%_@_%.__%',concat(email,','),''),iif(email2 like '%_@_%.__%',concat(email2,','),''),iif(email3 like '%_@_%.__%',email3,'')) as email from bullhorn1.BH_UserContact)
--, combinedmail as (select userID, iif(right(email,1)=',',left(email,len(email)-1),email) as combinedmail from contactmail)
--select * from combinedmail

  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
--, mail4 (ID,email,rn) as ( SELECT ID, email = CONVERT(NVARCHAR(MAX), email), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail3 where email like '%@%@%'

------------
-- COMMENT
------------
/*, comment(userID, comment) as (
	SELECT
		userID,
		left(replace(replace(replace(replace(
			STUFF((SELECT char(10) 
			+ 'DATE ADDED: ' + convert(varchar(10), dateAdded, 120) + char(10)
			+ iif(action = '' or action is null,'',concat('ACTION: ',action,char(10)))
			+ iif(cast(comments as varchar(max)) = '' or cast(comments as varchar(max)) is null,'',concat(char(10),'COMMENTS: ',char(10),cast(comments as varchar(max)),char(10)))
			+ '-----' + char(10)
		  	from bullhorn1.BH_UserComment WHERE userID = a.userID order by dateAdded desc FOR XML PATH ('')), 1, 1, '')
		  ,'&#x0D;',''),'&amp;gt;',''),'&gt',''),'&lt;','')
		  ,32765)  AS URLList
	FROM  [bullhorn1].[BH_UserComment] AS a
	GROUP BY a.userID) */

, UserCom as (select UC.userID, UC.commentingUserID, U.name, UC.dateAdded, UC.action, UC.comments
	from bullhorn1.BH_UserComment UC 
	left join bullhorn1.BH_User U on UC.commentingUserID = U.userID)

, comments as (SELECT
     userID,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar,dateAdded,120) + ' || ' 
		 + 'Commented by: ' + name + ' || ' + 'Action: ' + action + ' || ' + cast(comments as nvarchar(max))
          from UserCom
          WHERE userID = a.userID
		  order by dateAdded desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS comments
FROM UserCom as a
GROUP BY a.userID)

--select * from comments where userID = 162517

--select * from bullhorn1.BH_UserComment where userID = 162517

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
/* this is written for all custom required fields from Search Elect */
, note1 as (select userID
	, concat(iif(address1 = '' or address1 is NULL,'',concat('Address 1: ',address1,char(10)))
		,iif(city = '' or city is NULL,'',concat('City: ',city,char(10)))
		,iif(state = '' or state is NULL,'',concat('State: ',state,char(10)))
		,iif(cast(countryID as varchar(max)) = '' or countryID is NULL,'',concat('Country: ',tmp_country.COUNTRY,char(10)))
		,iif(employmentPreference = '' or employmentPreference is NULL,'',concat('Employment Preference: ',employmentPreference))
	) as note 
	from bullhorn1.BH_UserContact UC 
	left join tmp_country on UC.countryID = tmp_country.CODE)
--select * from note1

, note2 as (
	select userID
	,concat(
		 case when (Cl.status = '' OR Cl.status is NULL) THEN '' ELSE concat('Status: ',Cl.status,char(10)) END --as Status1
		, case when (Cl.division = '' OR Cl.division is NULL) THEN '' ELSE concat('Department: ',Cl.division,char(10)) END --as Department
		, case when (cast(Cl.desiredCategories as varchar(max)) = '' OR Cl.desiredCategories is NULL) THEN '' ELSE concat('Designed Categories: ',Cl.desiredCategories,char(10)) END --as DesignedCategories
		, case when (cast(Cl.desiredSpecialties as varchar(max)) = '' OR Cl.desiredSpecialties is NULL) THEN '' ELSE concat('Desired Specialties: ',Cl.desiredSpecialties,char(10)) END --as DesiredSpecialties
		, case when (cast(Cl.desiredSkills as varchar(max)) = '' OR Cl.desiredSkills is NULL) THEN '' ELSE concat('Desired Skills: ',Cl.desiredSkills) END --as DesiredSkills
	) as note
	from bullhorn1.BH_Client Cl where isPrimaryOwner = 1 and isDeleted = 0)

------------
-- PHONE
------------
, phone1(userID, phone) as (SELECT
	userID,
	STUFF(
		(SELECT iif(phone = '' or phone is NULL or phone like '%?%','',concat(phone,',')) + 
		iif(phone2 = '' or phone2 is NULL,'',concat(phone2,',')) + 
		iif(phone3 = '' or phone3 is NULL,'',concat(phone3,',')) +
		iif(mobile = '' or mobile is NULL,'',concat(mobile,',')) +
		iif(workPhone = '' or workPhone is NULL or workPhone = '0','', workPhone)
		from bullhorn1.BH_UserContact
		WHERE userID = a.userID
		FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
	, 1, 0, '')  AS URLList
FROM  bullhorn1.BH_UserContact AS a
GROUP BY a.userID)

, phone2 (userID, phone) as (SELECT
     userID,
	 iif(right(phone,1)=',',left(phone,len(phone)-1),phone)
	 from phone1)

------------
-- DOCUMENT
------------
, doc as (select a.clientContactUserID, concat(a.clientContactFileID,a.fileExtension) as contactFile
	from bullhorn1.View_ClientContactFile a
	where fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html'))

, contactdoc as (SELECT clientContactUserID, STUFF((SELECT DISTINCT ',' + contactFile 
from doc 
WHERE clientContactUserID = a.clientContactUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS contactFile 
FROM doc AS a GROUP BY a.clientContactUserID)

-----MAIN SCRIPT------
select  UC.clientCorporationID as 'contact-companyId'
	, Cl.clientID as 'contact-externalId'
	, Cl.userID as '(userID)'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'candidate-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'candidate-Lastname'
	, UC.middleName as 'contact-middleName'
	, UC2.email as 'contact-owners'
	, UC2.name as 'Owners Name'
	, p.phone as 'contact-phone'
	, mail5.email as 'contact-email'
	, UC.occupation as 'contact-jobTitle'
	, contactdoc.contactFile as 'contact-document'
	, concat('BH Contact ID: ',Cl.ClientID,char(10)
		, case when CL.isDeleted = 1 then concat('Contact is deleted: ',char(10)) else '' end
		, iif(UC2.name = '' or UC2.name is NULL,'',concat('BH Contact Owners: ',UC2.name,char(10)))
		, note1.note,char(10)
		, note2.note,char(10)
		, replace(replace(replace(replace(replace([dbo].[udf_StripHTML](UC.lastNote_denormalized),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"')
		) as 'contact-Note'
    , replace(replace(replace(replace(replace(ltrim(rtrim([dbo].[udf_StripHTML](c.comments))),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'contact-comment'
	, len(replace(c.comments,'&#x0D;','')) as '(length-contact-comment)'
	, Cl.isDeleted as '(isDeleted)'
-- select count(*) --22815
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
--left join combinedmail e ON Cl.userID = e.userID
left join mail5 ON Cl.userID = mail5.ID
left join comments c on Cl.userID = c.userID
left join note1 on Cl.userID = note1.userID
left join note2 on Cl.userID = note2.userID
left join phone2 p on Cl.userID = p.userID
left join contactdoc on Cl.userID = contactdoc.clientContactUserID
where isPrimaryOwner = 1
order by Cl.clientID desc