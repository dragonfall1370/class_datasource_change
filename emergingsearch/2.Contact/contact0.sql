
/*with 
tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email from bullhorn1.BH_UserContact )
#select * from tmp_1
#select userID, email, CHARINDEX(email,',',0) from tmp_1 

, tmp_2(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)	ELSE email END as email from tmp_1)

, tmp_3(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END)	ELSE email END as email from tmp_2)

, tmp_4(userID, Notes) as (SELECT userID, STUFF(
    (SELECT ' || ' + 'Action: ' + action + ' || ' + convert(varchar(10), dateAdded, 120) + ': ' + cast(comments as varchar(max))
     from [bullhorn1].[BH_UserComment] WHERE userID = a.userID FOR XML PATH ('')), 1, 4, '')  AS URLList
FROM  [bullhorn1].[BH_UserComment] AS a GROUP BY a.userID)
#select * from tmp_4



, tmp_7(userid, recruiterUserID) as (select userid, recruiterUserID from bullhorn1.BH_Client)
#select * from tmp_7

#, tmp_8(userid, recruiterUserID, email) as (select UC.userid, tmp_7.recruiterUserID, UC.email from bullhorn1.BH_UserContact UC left join tmp_7 ON UC.userID = tmp_7.recruiterUserID)
, tmp_8(userid, recruiterUserID_email) as (select distinct UC.userid, UC.email from bullhorn1.BH_UserContact UC left join tmp_7 ON UC.userID = tmp_7.recruiterUserID)
#select * from tmp_8

##############
, tmp_businessSectorIDList1 (userid, BussinessID) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS BussinessID 
FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.BH_UserContact CA where CA.status not like '%Archive%' and CA.status <> '' )t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
#select cast(BussinessID as int) from tmp_businessSectorIDList1

, tmp_businessSectorIDList2(userid, bussinesid) as (select distinct userid, cast(BussinessID as varchar(max)) from (select userid, BussinessID from tmp_businessSectorIDList1) a)
#select * from tmp_businessSectorIDList2

#, t1(userId, IndustryName) as (select tmp_1.userid, BS.name from tmp_1 inner join bullhorn1.BH_BusinessSector BS ON tmp_1.bussinesid = BS.businessSectorID)
#, t1(userid, name, bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
#, t1(userid,name,bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
#select * from t1 order by userid

, t(userid, name) as (select tmp_businessSectorIDList2.userid, cast(BS.name as varchar(max)) from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
#select * from t where userid = 3144 #order by userid 

, tmp_businessSectorIDList3(userid, name) as (SELECT userid, name = 
    STUFF((SELECT DISTINCT ', ' + name
           FROM t b 
           WHERE b.userid = a.userid 
          FOR XML PATH('')), 1, 2, '')
FROM t a
GROUP BY userid
)
#select * from tmp_businessSectorIDList3 where userid = 893
#select * from tmp_businessSectorIDList3 order by userid
##############
, tmp_5 as (select Cl.userID 
#, concat('Address: ',CC.address1,char(10),' ',CC.address2,char(10),' ',UC.city,char(10),' ',UC.state,char(10),' ',UC.zip,char(10)) as 'Address'
, concat('Address: ',CC.address1,' ',CC.address2,' ',UC.city,' ',UC.state,' ',UC.zip,char(10)) as 'Address'
, case when (Cl.division = '' OR Cl.division is NULL) THEN '' ELSE concat('Department: ',Cl.division,char(10)) END as Department
, case when (UC2.reportToUserID = '' OR UC2.reportToUserID is NULL) THEN '' ELSE concat('ReportToUserID: ',Cl.division,char(10)) END as ReportToUserID
, case when (Cl.status = '' OR Cl.status is NULL) THEN '' ELSE concat('Status: ',Cl.status,char(10)) END as Status1
, case when (cast(Cl.desiredCategories as varchar(max)) = '' OR Cl.desiredCategories is NULL) THEN '' ELSE concat('Designed Categories: ',Cl.desiredCategories,char(10)) END as DesignedCategories
, case when (cast(Cl.desiredSpecialties as varchar(max)) = '' OR Cl.desiredSpecialties is NULL) THEN '' ELSE concat('Desired Specialties: ',Cl.desiredSpecialties,char(10)) END as DesiredSpecialties
, case when (cast(Cl.comments as varchar(max)) = '' OR Cl.comments is NULL) THEN '' ELSE concat('Comments: ',Cl.comments,char(10)) END as Comments
, case when (UC2.referredByUserID = '' OR UC2.referredByUserID is NULL) THEN '' ELSE concat('ReferredByUserID: ',UC2.referredByUserID,char(10)) END as ReferredByUserID
, case when (cast(tmp_businessSectorIDList3.name as varchar(max)) = '' OR tmp_businessSectorIDList3.name is NULL) THEN '' ELSE REPLACE(REPLACE(concat('BusinessSectorName: ',tmp_businessSectorIDList3.name,char(10)), '&amp;', '&'), 'amp;', '') END as BusinessSectorName
, case when (cast(tmp_8.recruiterUserID_email as varchar(max)) = '' OR tmp_8.recruiterUserID_email is NULL) THEN '' ELSE concat('RecruiterUserEmail: ',tmp_8.recruiterUserID_email,char(10)) END as 'RecruiterUserEmail'
#, case when (cast(Cl.desiredSkills as varchar(max)) = '' OR Cl.desiredSkills is NULL) THEN '' ELSE concat('Desired Skills: ',Cl.desiredSkills) END as DesiredSkills
	from bullhorn1.BH_Client Cl 
	left join bullhorn1.BH_ClientCorporation CC ON Cl.clientCorporationID = CC.clientCorporationID
	left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
	left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
	left join tmp_businessSectorIDList3 ON UC.userid = tmp_businessSectorIDList3.userid
	#left join bullhorn1.BH_Candidate CA ON UC.userID = CA.userID
	left join tmp_4 d on Cl.userID = d.userID
	left join tmp_8 ON Cl.recruiterUserID = tmp_8.userid
	#where Cl.status not like '%Archive%' and Cl.status <> ''
	where Cl.isPrimaryOwner = 1
  )
#select * from tmp_5

, tmp_6 as (select userID, concat(Address,Status1,Department,ReportToUserID,DesignedCategories,DesiredSpecialties,Comments,ReferredByUserID,BusinessSectorName,RecruiterUserEmail) as CombinedNote from tmp_5)
#, tmp_6 as (select userID, concat(Status1,char(10),Department,char(10),ReportToUserID,char(10),DesignedCategories,char(10),DesiredSpecialties,char(10),ReferredByUserID,char(10),BusinessSectorName,char(10),RecruiterUserEmail,char(10)) as CombinedNote from tmp_5)
#select * from tmp_6
##############
#	, tmp_9 (userID,contactphone) as (select Cl.userid, 
#	concat( case when (cast(UC.mobile as varchar(max)) = '' OR cast(UC.mobile as varchar(max)) = ' ' OR UC.mobile is NULL) THEN '' ELSE UC.mobile END,char(10)
#	, case when UC.phone not LIKE '%[0-9]%' THEN '' ELSE concat(',',UC.phone) END,char(10)
#	, case when (UC.workPhone not LIKE '%[0-9]%' or UC.workPhone is NULL)THEN '' ELSE concat(',',UC.workPhone) END,char(10)
#	)) as 'contactphone'
#	from 
#	left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
#select * from tmp_9

##############
*/
select 
	 cp.ID as 'contact-externalId'
	, cp.Title as 'contact-Title'
	, cp.firstName as 'contact-firstName'
	#, cp.Surname as 'contact-lastName'
    , case when (cp.Surname = '' OR cp.Surname is NULL) THEN 'No Lastname' ELSE cp.Surname END as 'contact-lastName'
	, cp.position as 'contact-jobTitle'
	, cl.cliid as 'contact-companyId'
    #, cl.CliName as 'contact-companyname'
	#, 'ZA' as 'company-locationCountry'
	
    #, u.User as 'Owners-name'
    , u.udf1 as 'contact-owners'
    
	, cp.Email as 'contact-email'

	#, concat(CC.city,char(10),CC.state,char(10)) as 'company-locationName'
	#, concat(CC.address1,char(10),CC.address2,char(10)) as 'company-locationAddress'
    ##, cl.Address5 as 'contact-locationCity'
	##, cl.Address3 as 'contact-locationState'
	##, cl.PostCode as 'contact-locationZipCode'

	, concat(cp.mobile,case when (cp.Telephone = '' OR cp.Telephone is NULL) THEN '' ELSE concat(', ',cp.Telephone) END) as 'contact-phone'
			
	, replace(cp.Note,'>','') as 'contact-Note'
    , case when (cpl.Class = '' OR cpl.Class is NULL) THEN '' ELSE concat('Class: ',cpl.Class) END as 'contact-comment'


from emergingsearch.clipeople cp
left join emergingsearch.client cl on cp.CliID = cl.CliID
left join emergingsearch.clipeoclass cpl on cp.Class1 = cpl.ID
left join emergingsearch.users u ON cl.Owner = u.userid
/*
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
#left join bullhorn1.BH_Candidate CA ON UC.userID = CA.userID
#left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
left join tmp_country tc ON UC.countryID = tc.code
left join tmp_3 b on ltrim(rtrim(cast(UC.ownerUserIDList as nvarchar(max)))) = cast(b.userID as nvarchar(max))
left join tmp_3 ON Cl.userID = tmp_3.userID
#left join tmp_4 d on Cl.userID = d.userID
left join tmp_6 f on Cl.userID = f.userID
left join tmp_8 ON Cl.recruiterUserID = tmp_8.userid
#left join tmp_businessSectorIDList3 ON UC.userid = tmp_businessSectorIDList3.userid
#where UC.email LIKE '%_@_%_.__%' and c.email LIKE '%_@_%_.__%'
left JOIN bullhorn1.BH_ClientCorporation CC ON Cl.clientCorporationID = CC.clientCorporationID
where 
#Cl.clientCorporationID = 571
Cl.status not like '%Archive%' and Cl.status <> '' 
and Cl.isPrimaryOwner = 1
#and Cl.userID = 9535
#UC.status not like '%Archive%' and UC.status <> '' 
#Cl.clientid = 910
#CA.userID = 790
#and UC.userID = 1228
#and Cl.status not like '%Archive%' and Cl.status <> '' 
#and UC.userID = 790
#and Cl.clientid = 790 
#and Cl.recruiterUserID = tmp_8.userid

select cp.ID, cp.firstName, cp.Surname, cli.filename #count(*)
from emergingsearch.clipeople cp
left join emergingsearch.client cl on cp.CliID = cl.CliID
left join (SELECT cliID, GROUP_CONCAT(distinct(ID),'.',replace(FileName,',',' ')) as filename FROM clifiles GROUP BY cliID) cli on cli.CliID = cl.cliID where cli.filename is not null
left join (SELECT cliID, GROUP_CONCAT(distinct(ID),'.',replace(FileName,',',' ')) as filename FROM clifiles GROUP BY cliID) cli on cli.CliID = cl.cliID where cli.filename is not null

select * from  clifiles
select * from emergingsearch.clipeople cp

#select recruiterUserID, UC.email from bullhorn1.BH_Client CL
#left join bullhorn1.BH_UserContact UC on CL.recruiterUserID = UC.userID
*/