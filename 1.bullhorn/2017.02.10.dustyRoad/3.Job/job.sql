
with 
tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email from bullhorn1.BH_UserContact )
 --select * from tmp_1
 --select userID, email, CHARINDEX(email,',',0) from tmp_1
 , tmp_2(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)	ELSE email END as email from tmp_1)
 , tmp_3(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) 	THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END) ELSE email END as email from tmp_2)
 --select * from tmp_3
----------------------------
, tmp1 (categoryID,name) as (select categoryID,name from bullhorn1.BH_Category)
, tmp_businessSectorIDList1 (userid, BussinessID) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS BussinessID 
FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.BH_UserContact CA where CA.status not like '%Archive%' and CA.status <> '' )t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
--select cast(BussinessID as int) from tmp_businessSectorIDList1

, tmp_businessSectorIDList2(userid, bussinesid) as (select distinct userid, cast(BussinessID as varchar(max)) from (select userid, BussinessID from tmp_businessSectorIDList1) a)
--select * from tmp_businessSectorIDList2
--, t1(userId, IndustryName) as (select tmp_1.userid, BS.name from tmp_1 inner join bullhorn1.BH_BusinessSector BS ON tmp_1.bussinesid = BS.businessSectorID)
--, t1(userid, name, bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--, t1(userid,name,bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--select * from t1 order by userid
, t(userid, name) as (select tmp_businessSectorIDList2.userid, cast(BS.name as varchar(max)) from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--select * from t where userid = 3144 --order by userid 

, tmp_businessSectorIDList3(userid, name) as (SELECT userid, name = 
    STUFF((SELECT DISTINCT ', ' + name
           FROM t b 
           WHERE b.userid = a.userid 
          FOR XML PATH('')), 1, 2, '')
FROM t a
GROUP BY userid
)
--select * from tmp_businessSectorIDList3 where userid = 893
--select * from tmp_businessSectorIDList3 order by userid
----------------------------
, tmp_5 as (select 
JP.jobPostingID
, Cl.userID
----, CC.clientCorporationID
, case when (cast(tmp_businessSectorIDList3.name as varchar(max)) = '' OR tmp_businessSectorIDList3.name is NULL) THEN '' ELSE REPLACE(REPLACE(concat('BusinessSectorName: ',tmp_businessSectorIDList3.name,char(10)), '&amp;', '&'), 'amp;', '') END as 'BusinessSectorName'
, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE concat('Country: ',tc.abbreviation,char(10)) END as 'Country'
--, JP.clientCorporationID as  'JP.clientCorporationID'
, case when (JP.customText1 = '' OR JP.customText1 is NULL) THEN '' ELSE concat('CustomText: ',JP.customText1,char(10)) END as 'CustomText'
--, case when (UC.categoryID = '' OR UC.categoryID is NULL) THEN '' ELSE concat('CategoryID: ',UC.categoryID,char(10)) END as 'CategoryID'
, case when (t1.name = '' OR t1.name is NULL) THEN '' ELSE concat('CategoryName: ',t1.name,char(10)) END as 'CategoryName'
--, case when (t1.name = '' OR t1.name is NULL) THEN '' ELSE t1.name END as 'CategoryName'
--, UC.categoryIDList
, UC.specialtyIDList as 'SpecialtyIDList'
--, UC.skillIDList
, JP.skills as 'Skills'
, case when (Cl.status = '' OR Cl.status is NULL) THEN '' ELSE concat('Status: ',Cl.status,char(10)) END as Status
, case when (JP.isOpen = '' OR JP.isOpen is NULL) THEN '' when (JP.isOpen = 0) THEN  concat('IsOpen: Yesterday',char(10)) ELSE '' END as 'IsOpen'
--, JP.billRateCategoryID
, JP.reasonClosed as 'ReasonClosed'
, case when (JP.feeArrangement = '' OR JP.feeArrangement is NULL) THEN '' ELSE concat('FeeArrangement: ',JP.feeArrangement,char(10)) END as 'FeeArrangement'
, case when (JP.payRate = '' OR JP.payRate is NULL) THEN '' ELSE concat('PayRate: ',JP.payRate,char(10)) END as 'PayRate'
, case when (JP.salaryUnit = '' OR JP.salaryUnit is NULL) THEN '' ELSE concat('SalaryUnit: ',JP.salaryUnit,char(10)) END as 'SalaryUnit'
, case when (JP.clientBillRate = '' OR JP.clientBillRate is NULL) THEN '' ELSE concat('ClientBillRate: ',JP.clientBillRate,char(10)) END as 'ClientBillRate'
, case when (JP.yearsRequired = '' OR JP.yearsRequired is NULL) THEN '' ELSE concat('YearsRequired: ',JP.yearsRequired,char(10)) END as 'YearsRequired'
, JP.benefits as 'Benefits'
--, JP.markUpPercentage
, case when (JP.isinterviewRequired = '' OR JP.isinterviewRequired is NULL) THEN '' ELSE concat('IsinterviewRequired: ',JP.isinterviewRequired,char(10)) END as 'IsinterviewRequired'
, case when (JP.willRelocate = '' OR JP.willRelocate is NULL) THEN '' ELSE concat('WillRelocate: ',JP.willRelocate,char(10)) END as 'WillRelocate'
--, JP.contractInfoHeader
, case when (JP.durationWeeks = '' OR JP.durationWeeks is NULL) THEN '' ELSE concat('DurationWeeks: ',JP.durationWeeks,char(10)) END as 'DurationWeeks'
, case when (JP.willSponsor = '' OR JP.willSponsor is NULL) THEN '' ELSE concat('WillSponsor: ',JP.willSponsor,char(10)) END as 'WillSponsor'
, case when (JP.hoursPerWeek = '' OR JP.hoursPerWeek is NULL) THEN '' ELSE concat('HoursPerWeek: ',JP.hoursPerWeek,char(10)) END as 'HoursPerWeek'
, case when (JP.onSite = '' OR JP.onSite is NULL) THEN '' ELSE concat('OnSite: ',JP.onSite,char(10)) END as 'OnSite'
--, JP.descriptionInfoHeader
, concat('Address: ',JP.address,' ',JP.city,' ',JP.state,' ',JP.zip,char(10)) as 'Address'
, case when (JP.source = '' OR JP.source is NULL) THEN '' ELSE concat('Source: ',JP.source,char(10)) END as 'Source'

	from bullhorn1.BH_JobPosting JP 
	left join bullhorn1.BH_Client Cl ON JP.clientUserID = Cl.userID
	----left join bullhorn1.BH_ClientCorporation CC ON Cl.clientCorporationID = CC.clientCorporationID
	--left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
	left join bullhorn1.BH_UserContact UC ON Cl.userid = UC.userID
	left join tmp1 t1 ON UC.categoryID = t1.categoryID
	left join tmp_country tc ON UC.countryID = tc.code
	left join tmp_businessSectorIDList3 ON UC.userid = tmp_businessSectorIDList3.userid

	--left join bullhorn1.BH_Candidate CA ON UC.userID = CA.userID
	--left join tmp_4 d on Cl.userID = d.userID
	--left join tmp_8 ON Cl.recruiterUserID = tmp_8.userid
	--where Cl.status not like '%Archive%' and Cl.status <> ''
	where Cl.isPrimaryOwner = 1
  )

--select * from tmp_5
--select jobPostingID,count(*) from tmp_5 group by jobPostingID having count(*) > 1


, tmp_6 as (select jobPostingID, concat(BusinessSectorName,Country,CustomText,CategoryName,SpecialtyIDList,Skills,Status,IsOpen,ReasonClosed,FeeArrangement,PayRate,SalaryUnit,ClientBillRate,YearsRequired,Benefits,IsinterviewRequired,WillRelocate,DurationWeeks,WillSponsor,HoursPerWeek,OnSite,Address,Source) as CombinedNote from tmp_5)
--select * from tmp_6
--select jobPostingID, count(*) from tmp_6 group by jobPostingID having count(*) > 1


----------------------------
--/*
select JP.jobPostingID as 'position-externalId' 
	, Cl.userid as 'position-contactId'
	, CC.name as '(CompanyName)'
	, JP.UserID as 'UserID'
	, JP.title as 'position-title'
	, JP.numOpenings as 'position-headcount'
	, t3.email as 'position-owners' 
	--, a.type as 'position-type' -- This field only accepts: PERMANENT,INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT
	, UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(JP.employmentType, ' J2', ''), ' J3', ''), ' J4', ''), ' J5',''),' ','_'),'Contract', 'CONTRACT'),'Freelance', 'CONTRACT'),'Temp','TEMPORARY'),'Contract_to_Permanent', 'TEMPORARY_TO_PERMANENT'),'Talent_Brief', 'INTERIM_PROJECT_CONSULTING')) as 'position-type'	-- This field only accepts: FULL_TIME, PART_TIME, CASUAL
	--, a.employmentType as 'position-employmentType'	-- This field only accepts: FULL_TIME, PART_TIME, CASUAL
	, JP.salary as 'position-actualSalary'
	, JP.publicDescription as 'position-publicDescription'
	, JP.description as 'position-internalDescription'
	, CONVERT(VARCHAR(10),JP.startDate,110) as 'position-startDate'
	, JP.dateClosed as 'position-endDate'
	, t6.CombinedNote as 'position-Note'
--	*/
--	select *
from bullhorn1.BH_JobPosting JP
left join bullhorn1.BH_Client Cl on JP.clientUserID = Cl.userID
left join bullhorn1.BH_ClientCorporation CC on Cl.clientCorporationID = CC.clientCorporationID
left join tmp_3 t3 ON JP.userID = t3.userID
left join tmp_6 t6 on JP.jobPostingID = t6.jobPostingID
--where 1=1
--and a.status not like '%Archive%'
--and a.clientUserID in (8884, 9273)
--and b.isPrimaryOwner = 1
--where a.jobPostingID = 1096
--where b.userid = 9
--order by a.jobPostingID

-- select * from bullhorn1.BH_JobPosting where jobPostingID = 10
-- select  from bullhorn1.BH_JobPosting
-- select * from bullhorn1.BH_Usercontact where userID = 9

--select UC.name, * from bullhorn1.BH_Client CL left join bullhorn1.BH_Usercontact UC on CL.userID = UC.userID where CL.userID = 19295


--select userid, count(*) from bullhorn1.BH_Client
--where isPrimaryOwner = 1
--group by userID having count(*) > 1

/*
select jobPostingID,title,userID,countryID,externalCategoryID,clientUserID,externalID,clientCorporationID,status,address,city,state,zip 
from bullhorn1.BH_JobPosting where clientUserID = 690 --and status not like '%Archive%'
--title like '%CGI Artist%'
--title like '%Digital Designer - Senior%'

select CC.name as '(CompanyName)',Cl.clientid,Cl.userID,Cl.clientCorporationID,Cl.externalID,Cl.status from bullhorn1.BH_Client CL left join bullhorn1.BH_ClientCorporation CC on Cl.clientCorporationID = CC.clientCorporationID
 --where Cl.userid = 2976 and Cl.status not like '%Archive%'
 --where Cl.userid = 1295 and Cl.status not like '%Archive%'
 where Cl.userid = 690 and Cl.status not like '%Archive%'

select * from bullhorn1.BH_UserContact where userid = 690 and status not like '%Archive%' 
--and name like '%Samantha Rouse%'

--select jp.jobPostingID, cc.name ,jp.title from bullhorn1.BH_ClientCorporation cc,bullhorn1.BH_JobPosting jp where cc.clientCorporationID = jp.clientCorporationID and jp.jobPostingID = 19
*/
