
--ALTER DATABASE [DatabaseName] SET COMPATIBILITY_LEVEL = 130

/*
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
*/

with
mail1 (ID,email) as (
       select Cl.clientID --C.candidateID
	, replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
	--from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID
	from bullhorn1.BH_Client Cl left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
	cross apply string_split( concat(UC.email,' ',UC.email2,' ',UC.email3) ,' ')
	--where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and C.isdeleted <> 1 and C.status <> 'Archive'
	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and Cl.isdeleted <> 1 and Cl.status <> 'Archive'
--       select REFERENCE
--	, replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
--	from PROP_EMAIL
--	cross apply string_split(EMAIL_ADD,' ')
--	where EMAIL_ADD like '%_@_%.__%' and REFERENCE in (61065,43945)
	)
--select * from mail1 where email <> ''

, mail2 (ID,email,rn,ID_rn) as (
       select distinct ID
              , trim(' ' from email) as email
              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
              , row_number() over(partition by ID order by trim(' ' from email)) as ID_rn --distinct if contacts may have more than 1 email
	from mail1
	where email like '%_@_%.__%'
	)
--select * from mail2

, ed (ID,email) as (
       select ID
	      , case when rn > 1 then concat(email,'_',rn)
	else email end as email
	from mail2
	where email is not NULL and email <> ''
	and ID_rn = 1
	)
, e2 (ID,email) as (select ID, email from mail2 where ID_rn = 2)
, e3 (ID,email) as (select ID, email from mail2 where ID_rn = 3)	
--select * from mail2 where email like '%lburlovich@challenger.com.au%'



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
	  coalesce('BH Contact ID: ' + NULLIF(cast(UC.userID as varchar(max)), '') + char(10), '')
--       + coalesce('Email 2: ' + NULLIF(cast(e2.email as nvarchar(max)), '') + char(10), '')
--       + coalesce('Email 3: ' + NULLIF(e3.email, '') + char(10), '')
--       + coalesce('Phone: ' + NULLIF(cast(UC.Phone as nvarchar(max)), '') + char(10), '')
--       + concat('Phone: ',ltrim(Stuff( coalesce(' ' + NULLIF(UC.phone, ''), '')
--                       + coalesce(', ' + NULLIF(UC.phone2, ''), '')
--                       + coalesce(', ' + NULLIF(UC.phone3, ''), '')
--                       + coalesce(', ' + NULLIF(UC.mobile, ''), '')
--                       + coalesce(', ' + NULLIF(UC.workPhone, ''), '')
--                       , 1, 1, '') ), char(10))

+ Coalesce('Past Client Contact: ' + NULLIF(convert(nvarchar(max),uc.customTextBlock1), '') + char(10), '')
+ Coalesce('Last Visit: ' + NULLIF(convert(nvarchar(max),cl.dateLastVisit), '') + char(10), '')
--+ coalesce('Date Last Visit: ' + NULLIF(cast(cl.datelastvisit as nvarchar(max)), '') + char(10), '')
+ Coalesce('Fax: ' + NULLIF(convert(nvarchar(max),uc.fax), '') + char(10), '')
+ Coalesce('Prefix: ' + NULLIF(convert(nvarchar(max),uc.namePrefix), '') + char(10), '')
+ coalesce('Referred By: ' + NULLIF(cast(ref.name as nvarchar(max)), '') + char(10), '')
+ coalesce('Reports To: ' + NULLIF(cast(UC.reportToUserID as nvarchar(max)), '') + ' - ' + UC1.name + char(10), '')
--       + coalesce('Revenue: ' + NULLIF(cast(UC.revenue as nvarchar(max)), '') + char(10), '')
       + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),uc.status), '') + char(10), '')

--       + case when CL.isDeleted = 1 then concat('Contact is deleted: ',char(10)) else '' end
--       + coalesce('BH Contact Owners: ' + NULLIF(cast(UC2.name as nvarchar(max)), '') + char(10), '')
--       + coalesce('Contact Middle Name: ' + NULLIF(cast(UC.middlename as nvarchar(max)), '') + char(10), '')
--       + coalesce('Created By: ' + NULLIF(cast(UC2.name as nvarchar(max)), '') + char(10), '')    

--       + coalesce('Department: ' + NULLIF(cast(Cl.division as nvarchar(max)), '') + char(10), '')
--       + coalesce('Designed Categories: ' + NULLIF(cast(Cl.desiredCategories as nvarchar(max)), '') + char(10), '')
--       + coalesce('Desired Skills: ' + NULLIF(cast(Cl.desiredSkills as nvarchar(max)), '') + char(10), '')
--       + coalesce('Desired Specialties: ' + NULLIF(cast(Cl.desiredSpecialties as nvarchar(max)), '') + char(10), '')
--       + coalesce('Employment Preference: ' + NULLIF(cast(employmentPreference as nvarchar(max)), '') + char(10), '')
--       + coalesce('Fax: ' + NULLIF(cast(UC.Fax as nvarchar(max)), '') + char(10), '')
--       + coalesce('Preferred Communication Method: ' + NULLIF(cast(cl.preferredcontact as nvarchar(max)), '') + char(10), '')
--       + coalesce('Referred By: ' + NULLIF(cast(referredBy as nvarchar(max)), '') + char(10), '')
--       
--       

--       + coalesce('Secondary Owners: ' + NULLIF(cast(owner2c.name as nvarchar(max)), '') + char(10), '')
--       + coalesce('Source: ' + NULLIF(cast(UC.source as nvarchar(max)), '') + char(10), '')
--       + coalesce('StaffTrak Person ID: ' + NULLIF(cast(cl.externalid as nvarchar(max)), '') + char(10), '')
--       + coalesce('Status: ' + NULLIF(cast(Cl.status as nvarchar(max)), '') + char(10), '')
--       + coalesce('Status: ' + NULLIF(cast(UC.status as nvarchar(max)), '') + char(10), '')
--       + coalesce('Type: ' + NULLIF(cast(Cl.type as nvarchar(max)), '') + char(10), '')
--       + coalesce('Type: ' + NULLIF(cast(UC.Type as nvarchar(max)), '') + char(10), '')
--       + coalesce('Work Phone: ' + NULLIF(cast(UC.WorkPhone as nvarchar(max)), '') + char(10), '')

--       + coalesce('Address 1: ' + NULLIF(cast(address1 as nvarchar(max)), '') + char(10), '')
--       + coalesce('City: ' + NULLIF(cast(city as nvarchar(max)), '') + char(10), '')
--       + coalesce('State: ' + NULLIF(cast(state as nvarchar(max)), '') + char(10), '')
--       + coalesce('ZIP Code: ' + NULLIF(cast(zip as nvarchar(max)), '') + char(10), '')
--       + coalesce('Country: ' + NULLIF(tmp_country.COUNTRY, '') + char(10), '')

--       + coalesce('Last Note: ' + NULLIF( replace(replace(replace(replace(replace(replace([dbo].[udf_StripHTML](UC.lastNote_denormalized),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"'),'&gt;','') , '') + char(10), '') --cast(UC.lastNote_denormalized as varchar(max))
--       + coalesce('Last Note: ' + NULLIF(ltrim(rtrim([dbo].[udf_StripHTML]( 
--               replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
--                          cast(UC.lastNote_denormalized as varchar(max))
--              ,'&nbsp;','') ,'&ndash;','') ,'&amp;',''), '&hellip;','') ,'&#39;','') ,'&gt;','') ,'&lt;','') ,'&quot;','') ,'&rsquo;',''), '&ldquo;',''), '&rdquo;','') ,'&reg;','') ,'&euro;','')  ) )), '') + char(10), '') */
       + coalesce('General Comments: ' + NULLIF(cast(cl.comments as nvarchar(max)), '') + char(10), '') --General Comments
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
select --top 3
         Cl.clientID as 'contact-externalId', Cl.userID as '#UserID'
       , iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation /*where status <> 'Archive'*/), convert(varchar(max),UC.clientCorporationID), 'default' ) as 'contact-companyId' --, UC.clientCorporationID as 'contact-companyId'
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
       , UC.middleName as 'contact-middleName'
       , case 
              when Cl.status = 'Archive' and (trim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID,' (Archive)')
              when (trim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID)
              else trim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , UC2.email as 'contact-owners' --, UC2.name as '#Owners Name'
       , trim(Stuff( coalesce(' ' + NULLIF(UC.Phone, ''), '') , 1, 1, '') ) as 'contact-phone' --+ coalesce(', ' + NULLIF(UC.Phone2, ''), '') + coalesce(', ' + NULLIF(UC.Phone3, ''), '') + coalesce('' + NULLIF(UC.Mobile, ''), '') + coalesce(', ' + NULLIF(UC.WorkPhone, ''), '')
       --, UC.fax as 'contact-skype'
       , ed.email as 'contact-email' --iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email)
       , UC.occupation as 'contact-jobTitle'
       , doc.files as 'contact-document'
       , note.note as 'contact-note'
/*
       , Cl.dateadded as 'registration date'
       , UC.Mobile as 'mobile_phone'
       , ltrim(Stuff( coalesce(' ' + NULLIF(UC.Phone2, ''), '') + coalesce(', ' + NULLIF(UC.Phone3, ''), '') , 1, 1, '') ) as 'contact-home_phone' --, UC.Phone2 as 'home_phone' --, ltrim(Stuff( coalesce(' ' + NULLIF(UC.Phone2, ''), '') , 1, 0, '') ) as 'contact-home_phone'
       , Cl.division as 'Department'
       , e2.email as 'Personal Email'
       , UC.namePrefix as 'Tile'
       , UC.NickName as 'PreferredName'
       , Cl.desiredSkills as 'Skills'

       , UC.address1
       , UC.address2
       , UC.city
       , UC.state
       , UC.zip
       , UC.countryID
*/
-- select count(*) --7487 -- select distinct uc.source --convert(varchar(max),desiredSkills) as skills -- select top 10 *
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
left join ed ON Cl.clientID = ed.ID -- candidate-email-DUPLICATION
--left join e2 ON Cl.clientID = e2.ID -- candidate-email
--left join e3 ON Cl.clientID = e3.ID -- candidate-email
--left join e4 ON Cl.clientID = e4.ID -- candidate-email
left join note on Cl.clientID = note.clientID
left join doc on Cl.userID = doc.clientContactUserID
where (Cl.isdeleted <> 1)
--where (Cl.isdeleted <> 1 and Cl.status <> 'Archive') --where isPrimaryOwner = 1 --and UC.clientCorporationID = 1284
--and concat (UC.FirstName,' ',UC.LastName) like '%Partha%'
--and Cl.clientID in (3007,8,7,123,76,163)
--and Cl.userid in (57682, 57681, 57680)
--order by Cl.clientID desc


-- VINCERE - ARCHIVED CONTACT
select --top 3
         Cl.clientID as 'contact-externalId', Cl.userID as '#UserID', Cl.status
         , CC.clientCorporationID, CC.status
         , 1 as board, 2 as status
       , iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation where status = 'Archive'), convert(varchar(max),UC.clientCorporationID), 'default' ) as 'contact-companyId' --, UC.clientCorporationID as 'contact-companyId'
-- select count(CC.status) -- select distinct CC.clientCorporationID
-- select clientCorporationID, status from bullhorn1.BH_ClientCorporation where status = 'Archive' and clientCorporationID not in ( select CC.clientCorporationID
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_ClientCorporation CC on CC.clientCorporationID = UC.clientCorporationID
--where UC.clientCorporationID in (5,6,7,9,34,57,69,76,79,97,122,123,130,132,136,149,150,151,161,162,172,188,198,208,209,270,279,283,300,328,333,334,353,393,405,417,419,444,483,484,522,560,584,611,616,617,633,657,677,693,719,753,759,813,827,890,896,898,904,924,934,952,958,961,964,967,980,997,1009,1028,1031,1053,1055,1062,1074,1128,1133,1143,1196,1207,1220,1282,1290,1293,1301,1329,1347,1382,1416,1418,1426,1427,1471,1473,1480,1492,1545,1567,1579,1611,1678,1712,1715,1781,1817,1821,1827,1835,1838,2035,2062,2066,2185,2410,2476,2503,2596)
--where UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation where status = 'Archive')
where (Cl.isdeleted <> 1) 
and Cl.userid in (57682, 57681, 57680)
--where CC.status = 'Archive'
--
)

-- VINCERE ACTIVE -> PASSIVE
select --top 3
         Cl.clientID as 'contact-externalId', Cl.userID as '#UserID'
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
       , UC.middleName as 'contact-middleName'
       , case 
              when Cl.status = 'Archive' and (trim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID,' (Archive)')
              when (trim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID)
              else trim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , cl.isdeleted , CL.STATUS
       , 0 as active
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
WHERE CL.STATUS <> 'Active'


-- CREATE DEPARTMENT
select con.id as vincereID, con.external_id, con.first_name, con.middle_name, con.last_name, con.email, con.department
-- select *
from contact con 
where con.department is not null
and id = 36043

with t as (
select
       distinct iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation /*where status <> 'Archive'*/), convert(varchar(max),UC.clientCorporationID), 'default' ) as 'contact_companyId' --, UC.clientCorporationID as 'contact-companyId'
       , trim(replace(Cl.division,'_',' ')) as 'Department'
       
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where (Cl.isdeleted <> 1) and Cl.division is not null and Cl.division <> ''
--and Cl.clientID = 1382
)
select Department, count(*) from t group by Department having count(*) > 1

select
         Cl.clientID as 'contact-externalId' --, Cl.userID as '#UserID'
       , iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation /*where status <> 'Archive'*/), convert(varchar(max),UC.clientCorporationID), 'default' ) as 'contact-companyId' --, UC.clientCorporationID as 'contact-companyId'
--       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
--       , UC.middleName as 'contact-middleName'
       , trim(replace(Cl.division,'_',' ')) as 'Department'
       , lower(trim(replace(Cl.division,'_',' '))) as 'tmp'
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where (Cl.isdeleted <> 1)
and Cl.clientID = 1382


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
	, ltrim(Stuff(  coalesce(' ' + NULLIF(UC.Phone, '') + ' (Main)', '')
	               + coalesce(', ' + NULLIF(UC.Phone2, '') + ' (Direct)', '')
                , 1, 1, '') ) as 'contact-phone' 
	,UC.Mobile as 'Contact-MobilePhone'
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
where isPrimaryOwner = 1 )
select count(*) from t





with
-- desiredSkills
 s1 (clientID,desiredSkills) as ( SELECT clientID, trim(desiredSkills.value) as skills FROM bullhorn1.BH_Client m CROSS APPLY STRING_SPLIT( convert(varchar(max),m.desiredSkills),';') AS desiredSkills where convert(varchar(max),m.desiredSkills) <> '' )
--select distinct desiredSkills from s

-- desiredSpecialties
, s2 (clientID,desiredSpecialties) as ( SELECT clientID, trim(desiredSpecialties.value) as skills FROM bullhorn1.BH_Client m CROSS APPLY STRING_SPLIT( convert(varchar(max),m.desiredSpecialties),';') AS desiredSpecialties where convert(varchar(max),m.desiredSpecialties) <> '' )
--select distinct desiredSpecialties from s where desiredSpecialties not in ('.NET','3D','ActionScript','Activation','After Effects','APPS','Automotive','B2B','Branding','Business Services','C#','Cinema 4D','Consumer','Content','CSS','Financial','Flash','FREELANCE','HR','HTML','Javascript','-JUNIOR','-MIDWEIGHT','MySQL','Not For Profit','Online PR','PHP','PowerPoint','PPC','Professional Services','project management','Public Affairs','Public Sector','Retail','Personal Care','Brand','Digital','innovation chef','MS&P','OOH','Shopper Marketing','Field Sales','-SENIOR','SEO','Services','social','SQL Server','Utilities','Ambient','Cosmetics','Dairy','Drinks - Alcoholic','Drinks - Non Alcoholic','Fresh & Chilled','Frozen','Household','Buying','Ecommerce','Insights','Own label','National Accounts - Sales','Trade Marketing','Regional Sales','Snacking & Confectionary','Category','EMEA','Food service','International','NPD','Packaging','Travel/leisure','Consumer Marketing','Highstreet','Major Mulitples','Off-trade','Premium Multiples','Wholesale','Sales','Convenience/Discounters','Pharma/Heathcare2','Innovation','Marketing','On-trade','R&D','Business Development','Online')

, s3 as (
select
         Cl.clientID as 'contact-externalId' --, Cl.userID as '#UserID'
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
       , case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , s1.desiredSkills as 'skill'
-- select top 10 *
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join s1 on s1.clientID = Cl.clientID
where (s1.desiredSkills <> '' and s1.desiredSkills is not null)
--and Cl.clientID = 3617

UNION

select
         Cl.clientID as 'contact-externalId' --, Cl.userID as '#UserID'
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
       , case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , s2.desiredSpecialties as 'skill'
-- select distinct convert(varchar(max),cl.desiredSpecialties)
from bullhorn1.BH_Client Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join s2 on s2.clientID = Cl.clientID
where ( convert(varchar(max),cl.desiredSpecialties) <> '' and Cl.desiredSpecialties is not null)
--and Cl.clientID = 3617
)

select distinct skill, count(*) from s3 group by skill
*/

select distinct source from bullhorn1.BH_UserContact C where source is not null and source <> ''
with
source (userID,source) as (
       SELECT 
              userID
              , trim( replace(replace(replace(source.value,'  ',' '),' )',')'),'( ','(') ) as source --, trim( ind.value ) as ind 
       FROM (
              SELECT userID, trim( source.value ) as source 
              FROM bullhorn1.BH_UserContact m 
              CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.source) ), ',') AS source
              ) m
       CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.source) ), ';') AS source
       where (source is not null and convert(nvarchar(max),source) <> '' and source <> 'Please Select')
       )
--select distinct customText11 from customText11 where customText11 <> '#N/A'
select distinct source, count(*) from source where source <> '#N/A' group by source

select distinct trim(uc.source) as name
	   , lower(trim(uc.source)) as tmp
       , 1 as source_type
       , current_timestamp as insert_timestamp
       , 11 as payment_style 
from bullhorn1.BH_UserContact UC
--left join bullhorn1.BH_lead L on L.userid = UC.userid
left join bullhorn1.BH_Client cl ON cl.userID = uc.userID
where uc.source <> '' and uc.source <> '.' 
and uc.source is not null
--and c.dateAdded >= '2020-01-01 00:00:00'
*/