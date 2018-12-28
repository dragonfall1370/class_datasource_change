



with 
tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email from bullhorn1.BH_UserContact )
 --select * from tmp_1
 --select userID, email, CHARINDEX(email,',',0) from tmp_1
 , tmp_2(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)	ELSE email END as email from tmp_1)
 , tmp_3(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) 	THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END) ELSE email END as email from tmp_2)

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
, case when (JP.customText1 = '' OR JP.customText1 is NULL) THEN '' ELSE concat('Status: ',JP.customText1,char(10)) END as 'CustomText'
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
select * from tmp_6
--select jobPostingID, count(*) from tmp_6 group by jobPostingID having count(*) > 1

