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
	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') --and Cl.isdeleted <> 1 and Cl.status <> 'Archive'
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
	  Coalesce('BH Contact ID: ' + NULLIF(cast(UC.userID as varchar(max)), '') + char(10), '')
--       + Coalesce('Email: ' + NULLIF(cast(email2 as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Email 2: ' + NULLIF(cast(e2.email as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Email 3: ' + NULLIF(e3.email, '') + char(10), '')
--       + Coalesce('Past Companies: ' + NULLIF(convert(nvarchar(max),uc.customtext1), '') + char(10), '')
--       --+ Coalesce('Marketing Opt-In: ' + NULLIF(convert(nvarchar(max),uc.customtext10), '') + char(10), '')
--       + Coalesce('Source Details: ' + NULLIF(convert(nvarchar(max),uc.customText11), '') + char(10), '')
--       + Coalesce('Entered By: ' + NULLIF(convert(nvarchar(max),uc.customText3), '') + char(10), '')
--       --+ Coalesce('Source: ' + NULLIF(convert(nvarchar(max),uc.customText5), '') + char(10), '')
--       --+ Coalesce('Service Office: ' + NULLIF(convert(nvarchar(max),uc.customText6), '') + char(10), '')
--       --+ Coalesce('Date Last Visit: ' + NULLIF(cast(cl.datelastvisit as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Desired Skills: ' + NULLIF(cast(Cl.desiredSkills as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Department: ' + NULLIF(cast(Cl.division as nvarchar(max)), '') + char(10), '')
--       --+ Coalesce('StaffTrak Person ID: ' + NULLIF(cast(cl.externalid as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Fax: ' + NULLIF(cast(UC.Fax as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Contact Middle Name: ' + NULLIF(cast(UC.middlename as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Preferred Communication Method: ' + NULLIF(cast(cl.preferredcontact as nvarchar(max)), '') + char(10), '')
--       --+ Coalesce('Reports To: ' + NULLIF(cast(UC1.name as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Reports To: ' + NULLIF(cast(UC.reportToUserID as nvarchar(max)), '') + ' - ' + UC1.name + char(10), '')
--       + Coalesce('Status: ' + NULLIF(cast(UC.status as nvarchar(max)), '') + char(10), '')
--       --+ Coalesce('Status: ' + NULLIF(cast(Cl.status as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Type: ' + NULLIF(cast(Cl.type as nvarchar(max)), '') + char(10), '')
       --+ Coalesce('Type: ' + NULLIF(cast(UC.Type as nvarchar(max)), '') + char(10), '')
       + Coalesce('Critical Info: ' + NULLIF(cast(cl.comments as nvarchar(max)), '') + char(10), '') --General Comments

--       + case when CL.isDeleted = 1 then concat('Contact is deleted: ',char(10)) else '' end
--       + Coalesce('BH Contact Owners: ' + NULLIF(cast(UC2.name as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Created By: ' + NULLIF(cast(UC2.name as nvarchar(max)), '') + char(10), '')    
--       + Coalesce('Designed Categories: ' + NULLIF(cast(Cl.desiredCategories as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Desired Specialties: ' + NULLIF(cast(Cl.desiredSpecialties as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Employment Preference: ' + NULLIF(cast(employmentPreference as nvarchar(max)), '') + char(10), '')
--       + Coalesce('General Contact Comments: ' + NULLIF(cast(Cl.Comments as nvarchar(max)), '') + char(10), '')                
--       + Coalesce('Phone: ' + NULLIF(cast(UC.Phone as nvarchar(max)), '') + char(10), '')
--       + concat('Phone: ',ltrim(Stuff( Coalesce(' ' + NULLIF(UC.phone, ''), '')
--                       + Coalesce(', ' + NULLIF(UC.phone2, ''), '')
--                       + Coalesce(', ' + NULLIF(UC.phone3, ''), '')
--                       + Coalesce(', ' + NULLIF(UC.mobile, ''), '')
--                       + Coalesce(', ' + NULLIF(UC.workPhone, ''), '')
--                       , 1, 1, '') ), char(10))
--       + Coalesce('Referred By: ' + NULLIF(cast(referredBy as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Referred By: ' + NULLIF(cast(ref.name as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Revenue: ' + NULLIF(cast(UC.revenue as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Secondary Owners: ' + NULLIF(cast(owner2c.name as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Source: ' + NULLIF(cast(UC.source as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Work Phone: ' + NULLIF(cast(UC.WorkPhone as nvarchar(max)), '') + char(10), '')
--
--       + Coalesce('Address 1: ' + NULLIF(cast(address1 as nvarchar(max)), '') + char(10), '')
--       + Coalesce('City: ' + NULLIF(cast(city as nvarchar(max)), '') + char(10), '')
--       + Coalesce('State: ' + NULLIF(cast(state as nvarchar(max)), '') + char(10), '')
--       + Coalesce('ZIP Code: ' + NULLIF(cast(zip as nvarchar(max)), '') + char(10), '')
--       + Coalesce('Country: ' + NULLIF(tmp_country.COUNTRY, '') + char(10), '')
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
select --top 3
         Cl.clientID as 'contact-externalId' --, Cl.userID as '#UserID'
       , iif(UC.clientCorporationID in (select clientCorporationID from bullhorn1.BH_ClientCorporation where status <> 'Archive'), convert(varchar(max),UC.clientCorporationID), 'default' ) as 'contact-companyId' --, UC.clientCorporationID as 'contact-companyId'
       , case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
--       , UC.middleName as 'contact-middleName'
       , case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
       , UC2.email as 'contact-owners' --, UC2.name as '#Owners Name'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone, ''), '') , 1, 1, '') ) as 'contact-phone' --+ Coalesce(', ' + NULLIF(UC.Phone2, ''), '') + Coalesce(', ' + NULLIF(UC.Phone3, ''), '') + Coalesce('' + NULLIF(UC.Mobile, ''), '') + Coalesce(', ' + NULLIF(UC.WorkPhone, ''), '')
       --, UC.fax as 'contact-skype'
       , ed.email as 'contact-email' --iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) 
       , UC.occupation as 'contact-jobTitle'
       , doc.files as 'contact-document'
       , note.note as 'contact-note'

/*
       , Cl.dateadded as 'registration date'
       , UC.Mobile as 'mobile_phone'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '') + Coalesce(', ' + NULLIF(UC.Phone3, ''), '') , 1, 1, '') ) as 'contact-home_phone' --, UC.Phone2 as 'home_phone' --, ltrim(Stuff( Coalesce(' ' + NULLIF(UC.Phone2, ''), '') , 1, 0, '') ) as 'contact-home_phone'
       , Cl.division as 'Department'
       , e2.email as 'Personal Email'
       , UC.namePrefix as 'Tile'
       , UC.NickName as 'PreferredName'
       , Cl.desiredSkills as 'Skills'
       , UC.customText6 as 'Service Office - Put in Contact Information under Twitter '
       , cl.externalid  as 'StaffTrak Person ID'

       , UC.address1
       , UC.address2
       , UC.city
       , UC.state
       , UC.zip
       , UC.countryID
*/
-- select count(*) --39173 -- select distinct UC.customText5 --reportToUserID --convert(varchar(max),desiredSkills) as skills -- select customText6
from bullhorn1.BH_Client Cl --where (Cl.isdeleted = 1 or Cl.status = 'Archive')
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
left join ed ON Cl.clientID = ed.ID -- candidate-email-DUPLICATION
left join e2 ON Cl.clientID = e2.ID -- candidate-email
--left join e3 ON Cl.clientID = e3.ID -- candidate-email
--left join e4 ON Cl.clientID = e4.ID -- candidate-email
left join note on Cl.clientID = note.clientID
left join doc on Cl.userID = doc.clientContactUserID
where (Cl.isdeleted <> 1 and Cl.status <> 'Archive') --where isPrimaryOwner = 1 --and UC.clientCorporationID = 1284
--and UC.clientCorporationID not in (select distinct CC.clientcorporationid from bullhorn1.BH_ClientCorporation CC left join bullhorn1.BH_JobPosting j on j.clientcorporationid = cc.clientcorporationid where CC.countryID = 1 and j.clientcorporationid is null)

--and concat (UC.FirstName,' ',UC.LastName) like '%Partha%'
--and Cl.clientID in (4445)
--order by Cl.clientID desc


select distinct customText10 
from bullhorn1.BH_Client Cl --where (Cl.isdeleted <> 1 and Cl.status <> 'Archive')
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where customText10 is not null

select distinct customText5 
from bullhorn1.BH_Client Cl --where (Cl.isdeleted <> 1 and Cl.status <> 'Archive')
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where customText5 is not null

-- Service Office
select distinct customText6, count(*)
from bullhorn1.BH_Client Cl --where (Cl.isdeleted <> 1 and Cl.status <> 'Archive')
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where customText6 is not null 
group by customText6

SELECT
         candidate_externalId as additional_id, fullname 
        , 'add_con_info' as 'additional_type'
        , 1007 as 'form_id'
        , 11266 as 'field_id'
        , case
when 'Amsterdam' then '1'
when 'Cincinnati' then '2'
when 'CN Ottawa' then '3'
when 'CN Toronto' then '4'
when 'Ottawa' then '5'
when 'Toronto' then '6'
else '' end as 'field_value'
        , 11266 as 'constraint_id'
-- select count(*)        
from bullhorn1.BH_Client Cl --where (Cl.isdeleted <> 1 and Cl.status <> 'Archive')
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where customText6 is not null and customText6 <> '' and customText6 <> 'Please Select'

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

/*
select Cl.clientID
    , UC.reportToUserID, UC1.name, UC1.email, UC1.email2
-- select distinct UC1.email
from bullhorn1.BH_UserContact UC
left join bullhorn1.BH_Client Cl on Cl.Userid = UC.UserID
left join (select userID, name, email, email2 from bullhorn1.BH_UserContact) UC1 on UC.reportToUserID = UC1.userID
where Cl.Userid is not null and UC1.email is not null
*/