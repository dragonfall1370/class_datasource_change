--ALTER DATABASE BULLHORN7158_PRIME SET COMPATIBILITY_LEVEL = 140
with
  mail1 (ID,email) as (select Cl.clientID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
  concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3)))
  ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email 
		from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
		left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
		where (Cl.isdeleted <> 1 and Cl.status <> 'Archive') )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) 
		when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) 
		else email end as email 
		from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--select * from e1

--OWNERS
, owners as (select userID
				, trim(value) as ownerId
				from bullhorn1.BH_UserContact
				cross apply string_split(cast(ownerUserIDList as nvarchar(max)), ',')
				
				UNION
				
				select userID
				, recruiterUserID
				from bullhorn1.BH_Client)

, ownersfinal as (select o.userID
				, string_agg(uc.email, ',' ) as owners
				, string_agg(uc.name, ',' ) as ownerName
				from (select distinct userID, ownerId from owners) as o
				left join (select userid, name, email from bullhorn1.BH_UserContact) uc on uc.userID = o.ownerId
				group by o.userID)

--DOCUMENT
, doc (clientContactUserID,files) as ( SELECT clientContactUserID,
		STRING_AGG(cast(concat(clientContactFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY clientContactFileID) att 
		from bullhorn1.View_ClientContactFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ 
		GROUP BY clientContactUserID )
--select * from doc

--EDUCATION
, EducationSummary (userId, es) as (
       SELECT userId
       , STRING_AGG( cast(
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            concat_ws( char(10)
                                    , coalesce('Date Added: ' + nullif(convert(nvarchar(10), dateAdded, 120), ''), NULL)
									, coalesce('Start Date: ' + nullif(convert(nvarchar(10), startDate, 120), ''), NULL)
									, coalesce('End Date: ' + nullif(convert(nvarchar(10), endDate, 120), ''), NULL)
									, coalesce('School: ' + nullif(cast(school as nvarchar(max)), ''), NULL)
									, coalesce('Graduation Date: ' + nullif(convert(nvarchar(10), graduationDate, 120), ''), NULL)
									, coalesce('Major: ' + nullif(cast(major as nvarchar(max)), ''), NULL)
									, coalesce('Degree: ' + nullif(cast(degree as nvarchar(max)), ''), NULL)
									, coalesce('GPA: ' + nullif(cast(gpa as nvarchar(max)), ''), NULL)
									, coalesce('Certification: ' + nullif(cast(certification as nvarchar(max)), ''), NULL)
									, coalesce('Expiration Date: ' + nullif(convert(nvarchar(10), expirationDate, 120), ''), NULL)
									, coalesce('Comments: ' + nullif(cast(comments as nvarchar(max)), ''), NULL)
									, coalesce('City: ' + nullif(cast(city as nvarchar(max)), ''), NULL)
									, coalesce('County: ' + nullif(cast(state as nvarchar(max)), ''), NULL)
									--, coalesce('Education ID: ' + nullif(cast(userEducationID as nvarchar(max)), ''), NULL)
                            )
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as nvarchar(max))
       , concat(char(10),char(13)) ) WITHIN GROUP (ORDER BY dateadded) es
       FROM bullhorn1.BH_UserEducation GROUP BY userId       
       )

--EMPLOYMENT HISTORY
, EmploymentHistory(userId, eh) as (
       SELECT userId
         , STRING_AGG(
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            concat_ws(char(10)
							, coalesce('Title: ' + nullif(cast(title as nvarchar(max)), ''), NULL)
							, coalesce('Client Corporation: ' + nullif(cast(clientCorporationID as nvarchar(max)), ''), NULL)
							, coalesce('Company Name: ' + nullif(cast(companyName as nvarchar(max)), ''), NULL)
							, coalesce('Start Date: ' + nullif(convert(nvarchar(10), startDate, 120), ''), NULL)
							, coalesce('End Date: ' + nullif(convert(nvarchar(10), endDate, 120), ''), NULL)
							, coalesce('Date Added: ' + nullif(convert(nvarchar(10), dateAdded, 120), ''), NULL)
							, coalesce('Job Posting: ' + nullif(cast(title as nvarchar(max)), ''), NULL) --jobPostingID
							, coalesce('Placement: ' + nullif(cast(placementID as nvarchar(max)), ''), NULL)
							, coalesce('Termination Reason: ' + nullif(cast(terminationReason as nvarchar(max)), ''), NULL)
							, coalesce('Salary Type: ' + nullif(cast(salaryType as nvarchar(max)), ''), NULL)
							, coalesce('Salary Low: ' + nullif(cast(salary1 as nvarchar(max)), ''), NULL)
                            , coalesce('Salary High: ' + nullif(cast(salary2 as nvarchar(max)), ''), NULL)
							, coalesce('User Work History ID: ' + nullif(cast(userWorkHistoryID as nvarchar(max)), ''), NULL)
							, coalesce('Commission: ' + nullif(cast(commission as nvarchar(max)), ''), NULL)
							, coalesce('Bonus: ' + nullif(cast(bonus as nvarchar(max)), ''), NULL)
							, coalesce('Comments: ' + nullif(cast(comments as nvarchar(max)), ''), NULL)
                            ) 
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as eh
       , concat(char(10),char(13)) ) WITHIN GROUP (ORDER BY startDate desc, dateAdded desc) eh
       FROM bullhorn1.BH_userWorkHistory GROUP BY userId
       )

--NOTE
, note as (
	select Cl.clientID
	, concat_ws(char(10)  
		, coalesce('BH Contact ID: ' + NULLIF(cast(UC.userID as varchar(max)), ''), NULL)
		, coalesce('Title: ' + NULLIF(cast(UC.namePrefix as varchar(max)), ''), NULL)
		--, coalesce('Email 3: ' + NULLIF(e3.email, ''), NULL)
		, coalesce('Source: ' + NULLIF(cast(UC.source as varchar(max)), ''), NULL)
		--, coalesce('Status: ' + NULLIF(cast(UC.status as varchar(max)), ''), NULL)
		, coalesce('General Contact Comments: ' + NULLIF(cast(Cl.Comments as varchar(max)), ''), NULL)  
		--, coalesce('Priority: ' + NULLIF(cast(UC.customText2 as varchar(max)), ''), NULL)
		--, coalesce('Marketing Campaign: ' + NULLIF(cast(UC.customText3 as varchar(max)), ''), NULL)              
		--, coalesce('Fax: ' + NULLIF(cast(UC.Fax as varchar(max)), ''), NULL)
		, coalesce('Referred By: ' + NULLIF(cast(ref.name as varchar(max)), ''), NULL)
		--, coalesce('Reports to: ' + NULLIF(cast(UC1.name as varchar(max)), ''), NULL)
--      , coalesce('Secondary Owners: ' + NULLIF(cast(owner2c.name as varchar(max)), ''), NULL)
--		, coalesce('Type: ' + NULLIF(cast(UC.Type as varchar(max)), ''), NULL)
		, coalesce('Contact Type: ' + NULLIF(cast(Cl.type as varchar(max)), ''), NULL)
--		, coalesce('Email: ' + NULLIF(email2, ''), NULL)
--		+ case when CL.isDeleted = 1 then concat('Contact is deleted: ', char(10)) else '' end
--		+ concat('Phone: ',ltrim(Stuff( Coalesce(' ' + NULLIF(UC.phone, ''), '')
--		                , coalesce(', ' + NULLIF(UC.phone2, ''), '')
--		                , coalesce(', ' + NULLIF(UC.phone3, ''), '')
--		                --, coalesce(', ' + NULLIF(UC.mobile, ''), '')
--		                --, coalesce(', ' + NULLIF(UC.workPhone, ''), '')
--		                , 1, 1, '') ), char(10))
--		, coalesce('Phone: ' + NULLIF(UC.Phone, ''), NULL)
--		, coalesce('Work Phone: ' + NULLIF(UC.WorkPhone, ''), NULL)
--		, coalesce('Reports To: ' + NULLIF(cast(UC.reportToUserID as varchar(max)), '') + ' - ' + UC3.name + char(10), '')
--		, coalesce('Email 2: ' + NULLIF(e2.email, ''), NULL)
--		, coalesce('Department: ' + NULLIF(Cl.division, ''), NULL)
--		, coalesce('BH Contact Owners: ' + NULLIF(UC2.name, ''), NULL)
--		, coalesce('Address 1: ' + NULLIF(cast(address1 as varchar(max)), ''), NULL)
--		, coalesce('City: ' + NULLIF(city, ''), NULL)
--		, coalesce('State: ' + NULLIF(state, ''), NULL)
--		, coalesce('ZIP Code: ' + NULLIF(zip, ''), NULL)
--		, coalesce('Country: ' + NULLIF(tmp_country.COUNTRY, ''), NULL)
--		, coalesce('Referred By UserID: ' + NULLIF(cast(referredByUserID as varchar(max)), ''), NULL)
--		, coalesce('Referred By: ' + NULLIF(cast(referredBy as varchar(max)), ''), NULL)
		, coalesce('Date Last Visit: ' + NULLIF(convert(nvarchar(10), dateLastVisit, 120), ''), NULL)
--      , coalesce('Recruiter User ID: ' + NULLIF(cast(Cl.recruiterUserID as varchar(max)), ''), NULL)
--      , coalesce('Type: ' + NULLIF(cast(Cl.type as varchar(max)), ''), NULL)
--      , coalesce('Employment Preference: ' + NULLIF(employmentPreference, ''), NULL)
--      , coalesce('Status: ' + NULLIF(Cl.status, ''), NULL)
--      , coalesce('Designed Categories: ' + NULLIF(cast(Cl.desiredCategories as varchar(max)), ''), NULL)
--      , coalesce('Desired Specialties: ' + NULLIF(cast(Cl.desiredSpecialties as varchar(max)), ''), NULL)
--      , coalesce('Desired Skills: ' + NULLIF(cast(Cl.desiredSkills as varchar(max)), ''), NULL)
--      , coalesce('Lead Status: ' + NULLIF(cast(customText1 as varchar(max)), ''), NULL)
--      , coalesce('Reason Lost: ' + NULLIF(customText12, ''), NULL)
--      , coalesce('Company Previously Worked at: ' + NULLIF(customText15, ''), NULL)
--      , coalesce('Sold By: ' + NULLIF(customText11, ''), NULL)
--      , coalesce('Primary Candidates Looking For: ' + NULLIF(customText4, ''), NULL)
--      , coalesce('Secondary Candidates Looking For: ' + NULLIF(customText5, ''), NULL)
--      , coalesce('Tertiary Candidates Looking For: ' + NULLIF(customText13, ''), NULL)
--      , coalesce('Company Size: ' + NULLIF(customText18, ''), NULL)
--      , coalesce('Industry: ' + NULLIF(customText14, ''), NULL)
--      , coalesce('Last Note: ' + NULLIF( replace(replace(replace(replace(replace(replace([dbo].[udf_StripHTML](UC.lastNote_denormalized),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"'),'&gt;','') , ''), NULL)
--      , coalesce('Last Note: ' + NULLIF(ltrim(rtrim([dbo].[udf_StripHTML]( 
--               replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
--                          cast(UC.lastNote_denormalized as varchar(max))
--              ,'&nbsp;','') ,'&ndash;','') ,'&amp;',''), '&hellip;','') ,'&#39;','') ,'&gt;','') ,'&lt;','') ,'&quot;','') ,'&rsquo;',''), '&ldquo;',''), '&rdquo;','') ,'&reg;','') ,'&euro;','')  ) )), ''), NULL) */
		, coalesce('Education Summary: ' + NULLIF(es.es, ''), NULL)
		, coalesce('Work History: ' + NULLIF(eh.eh, ''), NULL)
		) as note 
        from bullhorn1.BH_UserContact UC --where name like '%Andy Teng%'
        left join tmp_country on UC.countryID = tmp_country.CODE
        left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
		left join EducationSummary es on es.userId = cl.userID --Education
		left join EmploymentHistory eh on eh.userId = cl.userID --Employment History
        left join (select userID,name from bullhorn1.BH_UserContact) UC1 on UC.reportToUserID = UC1.userID               
        left join (select userID,name from bullhorn1.BH_UserContact) UC2 on Cl.recruiterUserID = UC2.userID
        --left join e2 on Cl.userID = e2.ID
        left join e3 on Cl.userID = e3.ID
        left join (select userid, name from bullhorn1.BH_UserContact) ref on ref.userid = UC.referredByUserID
        left join ownersfinal o on o.userid = UC.userid
        --where Cl.isPrimaryOwner = 1 and Cl.isDeleted = 0
		where Cl.clientID is not NULL
        )

--REFERENCE (no data)

-----MAIN SCRIPT------
select 
         concat('PR',Cl.clientID) as 'contact-externalId'
       , iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation where status <> 'Archive')
				, concat('PR', UC.clientCorporationID), 'PR999999999' ) as 'contact-companyId' 
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' 
				else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
       , UC.middleName as 'contact-middleName'
       , case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname - ',Cl.clientID)
				else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , o.owners as 'contact-owners' --, o.ownerName as '#Owners Name'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone, ''), '') , 1, 1, '') ) as 'contact-phone'
       , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'contact-email'
       , UC.occupation as 'contact-jobTitle'
       , doc.files as 'contact-document'
       , note.note as 'contact-note'
       --, Cl.dateadded as 'registration date' --CUSTOM SCRIPT #
       --, UC.address1 --to be injected as CUSTOM SCRIPT
       --, UC.address2 --to be injected as CUSTOM SCRIPT
       --, UC.city --to be injected as CUSTOM SCRIPT
       --, UC.state --to be injected as CUSTOM SCRIPT
       --, UC.zip --to be injected as CUSTOM SCRIPT
       --, UC.countryID --to be injected as CUSTOM SCRIPT
       --, UC.Mobile as 'mobile_phone' --CUSTOM SCRIPT #
       --, ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '') + coalesce(', ' + NULLIF(UC.Phone3, ''), '') , 1, 1, '') ) as 'homephone' --CUSTOM SCRIPT #
       --, Cl.division as 'Department' --CUSTOM SCRIPT #
       ---, e2.email as 'Personal Email' --CUSTOM SCRIPT #
       --, UC.namePrefix as 'Tile' 
       --, UC.NickName as 'PreferredName'
       --, Cl.desiredSkills as 'Skils'
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join ownersfinal o on o.userID = cl.userID
left join ed ON Cl.clientID = ed.ID -- candidate-email | check duplication
left join e2 ON Cl.clientID = e2.ID -- candidate-email | Personal Email
--left join e3 ON Cl.clientID = e3.ID -- candidate-email
--left join e4 ON Cl.clientID = e4.ID -- candidate-email
left join note on Cl.clientID = note.clientID
left join doc on Cl.userID = doc.clientContactUserID
where (Cl.isdeleted <> 1 and Cl.status <> 'Archive') --where isPrimaryOwner = 1 --and UC.clientCorporationID = 1284
--and concat (UC.FirstName,' ',UC.LastName) like '%Partha%'
--and Cl.clientID in (3007,8,7,123,76,163)

UNION
select 'PR999999999','PR999999999','Default','','Contact','','','','','','This is default contact from data migration'